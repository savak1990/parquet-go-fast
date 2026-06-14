package parquetfast_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Concurrent decode tests. Run with -race to catch data races on the shared
// plan, pools, and the output slice. Correctness gate: a concurrent decode must
// produce exactly what the sequential decode produces.

func multiRGMerchants(t *testing.T, n, rowsPerRG int) []byte {
	t.Helper()

	rows := make([]merchantDay, n)
	for i := range rows {
		rows[i] = makeMerchant(i+1, i%7)
	}

	return writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(int64(rowsPerRG)))
}

func TestConcurrent_MatchesSequential(t *testing.T) {
	t.Parallel()

	buf := multiRGMerchants(t, 1000, 80) // ~13 row groups

	f, _ := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	if len(f.RowGroups()) < 2 {
		t.Fatalf("need ≥2 row groups, got %d", len(f.RowGroups()))
	}

	seq, err := parquetfast.UnmarshalBytes[merchantDay](buf)
	if err != nil {
		t.Fatalf("sequential: %v", err)
	}

	for _, workers := range []int{2, 4, 8, 64, 0 /* GOMAXPROCS */} {
		con, err := parquetfast.UnmarshalBytes[merchantDay](buf, parquetfast.WithConcurrency(workers))
		if err != nil {
			t.Fatalf("concurrent(%d): %v", workers, err)
		}

		if !reflect.DeepEqual(seq, con) {
			// find first diff
			for i := range seq {
				if i >= len(con) || !reflect.DeepEqual(seq[i], con[i]) {
					t.Fatalf("concurrent(%d) row %d differs from sequential", workers, i)
				}
			}

			t.Fatalf("concurrent(%d): length %d vs %d", workers, len(con), len(seq))
		}
	}
}

func TestConcurrent_SingleRowGroupFallback(t *testing.T) {
	t.Parallel()

	// One row group → concurrency can't split it; must still decode correctly.
	rows := make([]merchantDay, 200)
	for i := range rows {
		rows[i] = makeMerchant(i+1, i%4)
	}

	buf := writeGeneric(t, rows) // default: 1 row group

	f, _ := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	if len(f.RowGroups()) != 1 {
		t.Skipf("expected 1 row group, got %d", len(f.RowGroups()))
	}

	got, err := parquetfast.UnmarshalBytes[merchantDay](buf, parquetfast.WithConcurrency(8))
	if err != nil {
		t.Fatalf("concurrent: %v", err)
	}

	want, _ := parquetfast.UnmarshalBytes[merchantDay](buf)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("single-RG concurrent decode differs from sequential")
	}
}

func TestConcurrent_Empty(t *testing.T) {
	t.Parallel()

	buf := writeGeneric(t, []merchantDay{})

	got, err := parquetfast.UnmarshalBytes[merchantDay](buf, parquetfast.WithConcurrency(8))
	if err != nil {
		t.Fatalf("concurrent: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(got))
	}
}

// Million-row concurrent decode from disk (os.File ReadAt is concurrent-safe),
// verifying order-independent aggregates match the generated expectation.
func TestConcurrent_MillionFromFile(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping million-row concurrent test in -short mode")
	}

	path := filepath.Join(t.TempDir(), "merch_conc.parquet")
	want := writeMerchantFile(t, path, millionRows, 100_000) // ~10 row groups

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()

	fi, _ := f.Stat()

	got, err := parquetfast.Unmarshal[merchantDay](f, fi.Size(), parquetfast.WithConcurrency(0))
	if err != nil {
		t.Fatalf("concurrent unmarshal: %v", err)
	}

	if len(got) != millionRows {
		t.Fatalf("got %d rows, want %d", len(got), millionRows)
	}

	var agg merchAgg
	for i := range got {
		accMerchant(&agg, got[i])
	}

	if agg != want {
		t.Fatalf("aggregate mismatch:\n want %+v\n got  %+v", want, agg)
	}

	t.Logf("concurrent decoded %d rows (%d workers), %d products",
		agg.rows, runtime.GOMAXPROCS(0), agg.products)
}

// BenchmarkConcurrentDecode compares sequential vs concurrent decode of one
// multi-row-group file. Speedup scales with row-group count.
func BenchmarkConcurrentDecode(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in -short mode")
	}

	path := filepath.Join(b.TempDir(), "merch_conc_bench.parquet")
	writeMerchantFile(b, path, 400_000, 25_000) // 16 row groups

	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatalf("read: %v", err)
	}

	b.Logf("fixture: %d bytes, GOMAXPROCS=%d", len(data), runtime.GOMAXPROCS(0))

	b.Run("sequential", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[merchantDay](data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("concurrent", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[merchantDay](data, parquetfast.WithConcurrency(0)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
