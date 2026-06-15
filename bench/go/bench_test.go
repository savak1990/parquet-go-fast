package bench

import (
	"bytes"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// readFile slurps the benchmark file once; all benchmarks decode from memory so
// the OS page cache / disk is out of the measurement (warm-cache, decode-bound).
func readFile(tb testing.TB) []byte {
	tb.Helper()

	data, err := os.ReadFile(taxiPath())
	if err != nil {
		tb.Skipf("benchmark data missing (%v); run bench/download.sh", err)
	}

	return data
}

func reportRows(b *testing.B, totalRows int) {
	b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/s")
}

// ── Full materialization: decode every row, every column, into []struct ───────

func BenchmarkFull_Ours(b *testing.B) {
	data := readFile(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := 0

	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[Taxi](data)
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

func BenchmarkFull_OursConcurrent(b *testing.B) {
	data := readFile(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := 0

	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[Taxi](data, parquetfast.WithConcurrency(0))
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

func BenchmarkFull_ParquetGo(b *testing.B) {
	data := readFile(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := 0

	for b.Loop() {
		rows, err := parquet.Read[Taxi](bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

// (Projection is benchmarked across widths in projection_test.go.)

// ── Filtered scans ────────────────────────────────────────────────────────────
//
// For filtered workloads "output rows/s" is misleading (few rows out, whole-file
// scan in), so these report wall-time (ns/op) and the matched-row count. Two
// predicates of differing selectivity; results decode into TaxiProjection.

type filterCase struct {
	name string
	pred parquetfast.Predicate       // our pushdown predicate
	sql  string                      // DuckDB WHERE clause
	keep func(r TaxiProjection) bool // plain-Go filter (parquet-go path)
}

var filterCases = []filterCase{
	{
		name: "dist_gt_50",
		pred: parquetfast.Col("trip_distance").Greater(float64(50)),
		sql:  "trip_distance > 50",
		keep: func(r TaxiProjection) bool { return r.TripDistance > 50 },
	},
	{
		name: "fare_gt_100",
		pred: parquetfast.Col("fare_amount").Greater(float64(100)),
		sql:  "fare_amount > 100",
		keep: func(r TaxiProjection) bool { return r.FareAmount > 100 },
	},
}

func BenchmarkFilter_Ours(b *testing.B) {
	data := readFile(b)

	for _, fc := range filterCases {
		b.Run(fc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			var matched int

			for b.Loop() {
				rows, err := parquetfast.UnmarshalBytes[TaxiProjection](data, parquetfast.Where(fc.pred))
				if err != nil {
					b.Fatal(err)
				}

				matched = len(rows)
			}

			b.ReportMetric(float64(matched), "matched")
		})
	}
}

// parquet-go has no predicate pushdown via GenericReader, so the fair equivalent
// is to materialize all rows and filter in Go — what an application would have to
// do. (This decodes the whole file every time.)
func BenchmarkFilter_ParquetGo(b *testing.B) {
	data := readFile(b)

	for _, fc := range filterCases {
		b.Run(fc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			var matched int

			for b.Loop() {
				all, err := parquet.Read[TaxiProjection](bytes.NewReader(data), int64(len(data)))
				if err != nil {
					b.Fatal(err)
				}

				matched = 0

				for i := range all {
					if fc.keep(all[i]) {
						matched++
					}
				}
			}

			b.ReportMetric(float64(matched), "matched")
		})
	}
}
