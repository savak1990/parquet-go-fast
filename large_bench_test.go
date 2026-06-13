package parquetfast_test

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Large-data benchmark for the materialize-all APIs (UnmarshalBytes /
// UnmarshalFile) — the equivalent of ps-model's pqtReadRaw — versus parquet-go's
// reflection GenericReader reading the whole file into a []T. This is the
// "give me all the rows" comparison (not streaming), with and without
// concurrency. Skipped under -short.

const largeRows = 500_000

func BenchmarkLargeUnmarshal(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large-data benchmark in -short mode")
	}

	path := filepath.Join(b.TempDir(), "large.parquet")
	writeMerchantFile(b, path, largeRows, 50_000) // 10 row groups

	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatalf("read: %v", err)
	}

	b.Logf("fixture: %d rows, %d bytes, %d row groups, GOMAXPROCS=%d",
		largeRows, len(data), (largeRows+49_999)/50_000, runtime.GOMAXPROCS(0))

	// Baseline: parquet-go GenericReader, materialize every row into one []T.
	b.Run("parquet-go/GenericReader", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			r := parquet.NewGenericReader[merchantDay](bytes.NewReader(data))
			out := make([]merchantDay, largeRows)

			n := 0
			for n < len(out) {
				m, err := r.Read(out[n:])
				n += m

				if err != nil {
					break
				}
			}

			_ = r.Close()
		}
	})

	b.Run("parquet-go-fast/UnmarshalBytes", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[merchantDay](data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parquet-go-fast/UnmarshalBytes-concurrent", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[merchantDay](data, parquetfast.WithConcurrency(0)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parquet-go-fast/UnmarshalFile", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalFile[merchantDay](path); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parquet-go-fast/UnmarshalFile-concurrent", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalFile[merchantDay](path, parquetfast.WithConcurrency(0)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
