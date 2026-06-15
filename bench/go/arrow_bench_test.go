package bench

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func arrowOpen(tb testing.TB, data []byte) (*file.Reader, *pqarrow.FileReader) {
	tb.Helper()

	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		tb.Fatalf("open: %v", err)
	}

	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		tb.Fatalf("arrow reader: %v", err)
	}

	return pf, fr
}

func allRowGroups(pf *file.Reader) []int {
	rgs := make([]int, pf.NumRowGroups())
	for i := range rgs {
		rgs[i] = i
	}

	return rgs
}

// arrow-go (pure Go, no cgo) reads parquet into Arrow *columnar* arrays — the
// Go-native equivalent of pyarrow read_table. It does not produce row structs;
// the columnar read is one category (fast, no per-row objects), and transposing
// Arrow columns into []struct is a separate, row-materialization cost.

func arrowReadTableColumnar(tb testing.TB, data []byte) int64 {
	tb.Helper()

	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	defer func() { _ = pf.Close() }()

	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		tb.Fatalf("arrow reader: %v", err)
	}

	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		tb.Fatalf("read table: %v", err)
	}
	defer tbl.Release()

	return tbl.NumRows()
}

func BenchmarkFull_ArrowGoColumnar(b *testing.B) {
	data := readFile(b)
	b.ReportAllocs()
	b.ResetTimer()

	total := int64(0)
	for b.Loop() {
		total += arrowReadTableColumnar(b, data)
	}

	reportRows(b, int(total))
}

// colInt32/colFloat64 walk a named column's chunks and call set(rowIndex, value)
// for each non-null cell (nulls leave the Go zero value).
func colInt32(tbl arrow.Table, name string, set func(i int, v int32)) {
	col := tbl.Column(tbl.Schema().FieldIndices(name)[0])

	idx := 0
	for _, chunk := range col.Data().Chunks() {
		a := chunk.(*array.Int32)
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				set(idx, a.Value(i))
			}

			idx++
		}
	}
}

func colFloat64(tbl arrow.Table, name string, set func(i int, v float64)) {
	col := tbl.Column(tbl.Schema().FieldIndices(name)[0])

	idx := 0
	for _, chunk := range col.Data().Chunks() {
		a := chunk.(*array.Float64)
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				set(idx, a.Value(i))
			}

			idx++
		}
	}
}
