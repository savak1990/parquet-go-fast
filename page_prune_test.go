package parquetfast_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

type ppRow struct {
	ID  int64  `parquet:"id"`
	Cat string `parquet:"cat"`
	Pad string `parquet:"pad"`
}

func makePPRow(i int) ppRow {
	return ppRow{
		ID:  int64(i),
		Cat: []string{"a", "b", "c", "d"}[i%4],
		Pad: fmt.Sprintf("payload-%080d", i),
	}
}

// ppFixture builds a SINGLE row group with MANY pages (small PageBufferSize),
// ids monotonic — so each page covers a contiguous id range and page pruning can
// skip pages. One row group means row-group pruning can't fire, isolating the
// page-pruning effect.
func ppFixture(t *testing.T, n int) []byte {
	t.Helper()

	rows := make([]ppRow, n)
	for i := range rows {
		rows[i] = makePPRow(i)
	}

	return writeGeneric(t, rows,
		parquet.PageBufferSize(4096),
		parquet.MaxRowsPerRowGroup(int64(n)+1), // single row group
	)
}

func streamAll[T any](t *testing.T, rd *parquetfast.Reader[T]) []T {
	t.Helper()

	var out []T

	buf := make([]T, 64)
	for {
		n, err := rd.Read(buf)
		out = append(out, buf[:n]...)

		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("stream read: %v", err)
		}
	}

	return out
}

func TestPagePruning_SingleGroupReadsFewerBytes(t *testing.T) {
	t.Parallel()

	data := ppFixture(t, 20000)

	// Confirm single row group with multiple pages (so it's page pruning).
	f, _ := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if len(f.RowGroups()) != 1 {
		t.Skipf("expected 1 row group, got %d", len(f.RowGroups()))
	}

	cc := f.RowGroups()[0].ColumnChunks()[0].(*parquet.FileColumnChunk)
	ci, err := cc.ColumnIndex()
	if err != nil || ci == nil || ci.NumPages() < 2 {
		t.Skip("fixture lacks a multi-page column index")
	}

	t.Logf("single row group, %d pages on the id column", ci.NumPages())

	read := func(opts ...parquetfast.Option) int64 {
		cr := newRemoteReaderAt(data)
		if _, err := parquetfast.Unmarshal[ppRow](cr, int64(len(data)), opts...); err != nil {
			t.Fatalf("decode: %v", err)
		}

		return cr.bytes.Load()
	}

	full := read()
	pruned := read(parquetfast.Where(parquetfast.Col("id").Between(int64(5000), int64(5100))))

	t.Logf("bytes read: full=%d, page-pruned=%d (%.1f%% of full)",
		full, pruned, 100*float64(pruned)/float64(full))

	if pruned >= full {
		t.Fatalf("page pruning should read fewer bytes: %d >= %d", pruned, full)
	}

	// Correctness.
	got, _ := parquetfast.UnmarshalBytes[ppRow](data,
		parquetfast.Where(parquetfast.Col("id").Between(int64(5000), int64(5100))))
	if len(got) != 101 {
		t.Fatalf("expected 101 rows, got %d", len(got))
	}

	for _, r := range got {
		if r.ID < 5000 || r.ID > 5100 {
			t.Fatalf("row out of range: %d", r.ID)
		}
	}
}

// The same filtered query must give identical results through every public API:
// UnmarshalBytes, UnmarshalFile, Unmarshal (remote io.ReaderAt / S3), and the
// streaming Reader.
func TestPagePruning_AllAPIsAgree(t *testing.T) {
	t.Parallel()

	data := ppFixture(t, 10000)
	pred := parquetfast.Where(parquetfast.Col("id").Between(int64(3000), int64(3050)))

	// Expected via a manual filter of a full decode.
	all, err := parquetfast.UnmarshalBytes[ppRow](data)
	if err != nil {
		t.Fatalf("full: %v", err)
	}

	var want []ppRow
	for _, r := range all {
		if r.ID >= 3000 && r.ID <= 3050 {
			want = append(want, r)
		}
	}

	equal := func(name string, got []ppRow) {
		t.Helper()

		if len(got) != len(want) {
			t.Fatalf("%s: count %d, want %d", name, len(got), len(want))
		}

		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s row %d: got %+v want %+v", name, i, got[i], want[i])
			}
		}
	}

	// UnmarshalBytes
	got, err := parquetfast.UnmarshalBytes[ppRow](data, pred)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}
	equal("UnmarshalBytes", got)

	// UnmarshalFile
	path := filepath.Join(t.TempDir(), "pp.parquet")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	got, err = parquetfast.UnmarshalFile[ppRow](path, pred)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}
	equal("UnmarshalFile", got)

	// Unmarshal over a remote-style io.ReaderAt (S3 stand-in)
	got, err = parquetfast.Unmarshal[ppRow](newRemoteReaderAt(data), int64(len(data)), pred)
	if err != nil {
		t.Fatalf("Unmarshal(remote): %v", err)
	}
	equal("Unmarshal(remote)", got)

	// Streaming Reader
	rd, err := parquetfast.NewReader[ppRow](bytes.NewReader(data), int64(len(data)), pred)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()
	equal("Reader(stream)", streamAll(t, rd))
}

func TestPagePruning_StreamingReaderRemote(t *testing.T) {
	t.Parallel()

	data := ppFixture(t, 10000)

	// Streaming filtered read from a remote-style reader, counting bytes.
	rr := newRemoteReaderAt(data)

	rd, err := parquetfast.NewReader[ppRow](rr, int64(len(data)),
		parquetfast.Where(parquetfast.Col("id").Between(int64(8000), int64(8020))))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()

	got := streamAll(t, rd)

	t.Logf("streamed %d rows, fetched %d bytes of %d", len(got), rr.bytes.Load(), len(data))

	if len(got) != 21 {
		t.Fatalf("expected 21 rows, got %d", len(got))
	}

	for _, r := range got {
		if r.ID < 8000 || r.ID > 8020 {
			t.Fatalf("row out of range: %d", r.ID)
		}
	}

	if rr.bytes.Load() >= int64(len(data)) {
		t.Fatalf("streaming filter should fetch less than the whole file")
	}
}

func TestPagePruning_MultiPredicate(t *testing.T) {
	t.Parallel()

	data := ppFixture(t, 10000)

	// id range narrows by pages; cat is unclustered (won't page-prune) but must
	// still filter rows correctly via the intersection + per-row check.
	got, err := parquetfast.UnmarshalBytes[ppRow](data, parquetfast.Where(
		parquetfast.Col("id").Between(int64(2000), int64(2100)),
		parquetfast.Col("cat").Equal("a"),
	))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, r := range got {
		if r.ID < 2000 || r.ID > 2100 || r.Cat != "a" {
			t.Fatalf("row violates predicate: %+v", r)
		}
	}

	// ids 2000..2100 with i%4==0 (cat "a").
	expected := 0
	for i := 2000; i <= 2100; i++ {
		if i%4 == 0 {
			expected++
		}
	}

	if len(got) != expected {
		t.Fatalf("expected %d rows, got %d", expected, len(got))
	}
}

func BenchmarkPagePruning(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in -short mode")
	}

	const n = 1_000_000

	rows := make([]ppRow, n)
	for i := range rows {
		rows[i] = makePPRow(i)
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

	b.Run("full-decode", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[ppRow](data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("page-pruned-narrow-range", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := parquetfast.UnmarshalBytes[ppRow](data,
				parquetfast.Where(parquetfast.Col("id").Between(int64(500_000), int64(510_000))))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
