package parquetfast_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// countingAt wraps any io.ReaderAt and counts bytes read — a stand-in for remote
// storage to show how little a selective query on a sorted file actually fetches.
type countingAt struct {
	ra    io.ReaderAt
	bytes atomic.Int64
}

func (c *countingAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.ra.ReadAt(p, off)
	c.bytes.Add(int64(n))

	return n, err
}

// On a sorted column, page selection is a binary search over the ascending page
// index. The result must match the linear scan exactly; this checks every
// operator against a manual filter on a many-page sorted file.
func TestSorted_BinarySearchCorrectness(t *testing.T) {
	data := ppFixture(t, 20000) // single row group, many pages, ids ascending

	f, _ := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	cc := f.RowGroups()[0].ColumnChunks()[0].(*parquet.FileColumnChunk)

	ci, err := cc.ColumnIndex()
	if err != nil || ci == nil || !ci.IsAscending() {
		t.Skip("id column is not an ascending multi-page index")
	}

	t.Logf("single row group, %d ascending pages → binary search engaged", ci.NumPages())

	all, err := parquetfast.UnmarshalBytes[ppRow](data)
	if err != nil {
		t.Fatalf("full decode: %v", err)
	}

	check := func(name string, pred parquetfast.Predicate, want func(ppRow) bool) {
		t.Helper()

		got, err := parquetfast.UnmarshalBytes[ppRow](data, parquetfast.Where(pred))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		var exp []ppRow
		for _, r := range all {
			if want(r) {
				exp = append(exp, r)
			}
		}

		if len(got) != len(exp) {
			t.Fatalf("%s: count %d, want %d", name, len(got), len(exp))
		}

		for i := range exp {
			if got[i] != exp[i] {
				t.Fatalf("%s row %d: got %+v want %+v", name, i, got[i], exp[i])
			}
		}
	}

	check("eq", parquetfast.Col("id").Equal(int64(12345)), func(r ppRow) bool { return r.ID == 12345 })
	check("eq-page-boundary", parquetfast.Col("id").Equal(int64(512)), func(r ppRow) bool { return r.ID == 512 })
	check("between", parquetfast.Col("id").Between(int64(5000), int64(5100)), func(r ppRow) bool { return r.ID >= 5000 && r.ID <= 5100 })
	check("between-spanning", parquetfast.Col("id").Between(int64(499), int64(2001)), func(r ppRow) bool { return r.ID >= 499 && r.ID <= 2001 })
	check("ge", parquetfast.Col("id").GreaterOrEqual(int64(19990)), func(r ppRow) bool { return r.ID >= 19990 })
	check("gt", parquetfast.Col("id").Greater(int64(19990)), func(r ppRow) bool { return r.ID > 19990 })
	check("le", parquetfast.Col("id").LessOrEqual(int64(3)), func(r ppRow) bool { return r.ID <= 3 })
	check("lt", parquetfast.Col("id").Less(int64(5)), func(r ppRow) bool { return r.ID < 5 })
	check("none", parquetfast.Col("id").Greater(int64(1_000_000)), func(r ppRow) bool { return false })
	// NotEqual falls back to the linear scan (not a single interval) but must
	// still be correct on a sorted column.
	check("ne", parquetfast.Col("id").NotEqual(int64(7)), func(r ppRow) bool { return r.ID != 7 })
}

// ── The multi-GB demonstration (opt in with PARQUET_FAST_HUGE=1) ──────────────

type hugeRow struct {
	ID  int64  `parquet:"id"`
	Pad []byte `parquet:"pad"`
}

func TestSorted_HugeFile(t *testing.T) {
	if os.Getenv("PARQUET_FAST_HUGE") == "" {
		t.Skip("set PARQUET_FAST_HUGE=1 to run the multi-GB sorted-file test")
	}

	const (
		padBytes  = 120
		rowBytes  = padBytes + 8
		targetGiB = 2
		n         = (targetGiB << 30) / rowBytes // ~16.7M rows ≈ 2 GiB uncompressed
	)

	path := filepath.Join(t.TempDir(), "huge_sorted.parquet")

	// Generate: ids ascending (so the page index is ascending), uncompressed for
	// predictable size, small pages → many pages per row group.
	genStart := time.Now()
	writeHugeSorted(t, path, n, padBytes)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	t.Logf("generated %d rows, %.2f GiB in %s", n, float64(fi.Size())/(1<<30), time.Since(genStart).Round(time.Millisecond))

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = file.Close() }()

	cr := &countingAt{ra: file}

	// A narrow range deep in the file: row-group pruning skips all but one group,
	// then binary search picks the matching page(s) within it.
	loID, hiID := int64(n/2), int64(n/2+100)

	qStart := time.Now()
	got, err := parquetfast.Unmarshal[hugeRow](cr, fi.Size(),
		parquetfast.Where(parquetfast.Col("id").Between(loID, hiID)))
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	read := cr.bytes.Load()
	t.Logf("query [%d,%d]: %d rows in %s, read %.2f MiB of %.2f GiB (%.4f%%)",
		loID, hiID, len(got), time.Since(qStart).Round(time.Millisecond),
		float64(read)/(1<<20), float64(fi.Size())/(1<<30), 100*float64(read)/float64(fi.Size()))

	if len(got) != int(hiID-loID+1) {
		t.Fatalf("expected %d rows, got %d", hiID-loID+1, len(got))
	}

	for _, r := range got {
		if r.ID < loID || r.ID > hiID {
			t.Fatalf("row out of range: %d", r.ID)
		}
	}

	// The whole point: a selective query reads a tiny fraction of a multi-GB file.
	if read >= fi.Size()/10 {
		t.Fatalf("expected to read <10%% of the file, read %d of %d", read, fi.Size())
	}
}

func writeHugeSorted(t *testing.T, path string, n, padBytes int) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()

	w := parquet.NewGenericWriter[hugeRow](f,
		parquet.PageBufferSize(128<<10),       // many pages
		parquet.MaxRowsPerRowGroup(1_000_000), // bounded write memory + multiple groups
	)

	pad := make([]byte, padBytes)
	for i := range pad {
		pad[i] = byte(i)
	}

	const batch = 8192

	rows := make([]hugeRow, batch)

	for written := 0; written < n; {
		k := batch
		if k > n-written {
			k = n - written
		}

		for i := 0; i < k; i++ {
			rows[i] = hugeRow{ID: int64(written + i), Pad: pad}
		}

		if _, err := w.Write(rows[:k]); err != nil {
			t.Fatalf("write: %v", err)
		}

		written += k
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func BenchmarkSortedFilter(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in -short mode")
	}

	const n = 1_000_000

	rows := make([]ppRow, n)
	for i := range rows {
		rows[i] = makePPRow(i) // ids ascending
	}

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[ppRow](&buf,
		parquet.Compression(&parquet.Snappy),
		parquet.PageBufferSize(8192),
		parquet.MaxRowsPerRowGroup(n+1), // single row group, many pages
	)
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()

	b.Run("sorted-binary-search-narrow", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := parquetfast.UnmarshalBytes[ppRow](data,
				parquetfast.Where(parquetfast.Col("id").Between(int64(500_000), int64(500_100))))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
