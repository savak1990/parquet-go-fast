package bench

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Projection width sweep: read 1 / 5 / 10 of the 19 columns into Go structs.
// Each width is benchmarked across this library, parquet-go, arrow-go (columnar
// read transposed to rows), and DuckDB-via-Go (query → []struct). Output is
// always a ready-to-use Go slice, so the comparison is within-category.

type Taxi1 struct {
	TripDistance float64 `parquet:"trip_distance"`
}

type Taxi5 struct {
	PULocationID int32   `parquet:"PULocationID"`
	DOLocationID int32   `parquet:"DOLocationID"`
	TripDistance float64 `parquet:"trip_distance"`
	FareAmount   float64 `parquet:"fare_amount"`
	TotalAmount  float64 `parquet:"total_amount"`
}

type Taxi10 struct {
	PassengerCount int64   `parquet:"passenger_count"`
	TripDistance   float64 `parquet:"trip_distance"`
	RatecodeID     int64   `parquet:"RatecodeID"`
	PULocationID   int32   `parquet:"PULocationID"`
	DOLocationID   int32   `parquet:"DOLocationID"`
	PaymentType    int64   `parquet:"payment_type"`
	FareAmount     float64 `parquet:"fare_amount"`
	TipAmount      float64 `parquet:"tip_amount"`
	TollsAmount    float64 `parquet:"tolls_amount"`
	TotalAmount    float64 `parquet:"total_amount"`
}

// ── generic Go decoders (work for any row struct) ─────────────────────────────

func benchOurs[T any](b *testing.B, data []byte) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		rows, err := parquetfast.UnmarshalBytes[T](data)
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

func benchParquetGo[T any](b *testing.B, data []byte) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		rows, err := parquet.Read[T](bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}

		total += len(rows)
	}

	reportRows(b, total)
}

func BenchmarkProj_Ours(b *testing.B) {
	data := readFile(b)
	b.Run("01col", func(b *testing.B) { benchOurs[Taxi1](b, data) })
	b.Run("05col", func(b *testing.B) { benchOurs[Taxi5](b, data) })
	b.Run("10col", func(b *testing.B) { benchOurs[Taxi10](b, data) })
}

func BenchmarkProj_ParquetGo(b *testing.B) {
	data := readFile(b)
	b.Run("01col", func(b *testing.B) { benchParquetGo[Taxi1](b, data) })
	b.Run("05col", func(b *testing.B) { benchParquetGo[Taxi5](b, data) })
	b.Run("10col", func(b *testing.B) { benchParquetGo[Taxi10](b, data) })
}

// ── arrow-go: columnar read of the projection columns, transposed to rows ─────

func colInt64(tbl arrow.Table, name string, set func(i int, v int64)) {
	col := tbl.Column(tbl.Schema().FieldIndices(name)[0])

	idx := 0
	for _, chunk := range col.Data().Chunks() {
		a := chunk.(*array.Int64)
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				set(idx, a.Value(i))
			}

			idx++
		}
	}
}

func arrowProjTable(tb testing.TB, data []byte, cols []int) (arrow.Table, func()) {
	tb.Helper()

	pf, fr := arrowOpen(tb, data)

	tbl, err := fr.ReadRowGroups(context.Background(), cols, allRowGroups(pf))
	if err != nil {
		tb.Fatalf("read row groups: %v", err)
	}

	return tbl, func() {
		tbl.Release()
		_ = pf.Close()
	}
}

func arrowRows1(tb testing.TB, data []byte) int {
	tbl, done := arrowProjTable(tb, data, []int{4})
	defer done()

	out := make([]Taxi1, tbl.NumRows())
	colFloat64(tbl, "trip_distance", func(i int, v float64) { out[i].TripDistance = v })

	return len(out)
}

func arrowRows5(tb testing.TB, data []byte) int {
	tbl, done := arrowProjTable(tb, data, []int{4, 7, 8, 10, 16})
	defer done()

	out := make([]Taxi5, tbl.NumRows())
	colInt32(tbl, "PULocationID", func(i int, v int32) { out[i].PULocationID = v })
	colInt32(tbl, "DOLocationID", func(i int, v int32) { out[i].DOLocationID = v })
	colFloat64(tbl, "trip_distance", func(i int, v float64) { out[i].TripDistance = v })
	colFloat64(tbl, "fare_amount", func(i int, v float64) { out[i].FareAmount = v })
	colFloat64(tbl, "total_amount", func(i int, v float64) { out[i].TotalAmount = v })

	return len(out)
}

func arrowRows10(tb testing.TB, data []byte) int {
	tbl, done := arrowProjTable(tb, data, []int{3, 4, 5, 7, 8, 9, 10, 13, 14, 16})
	defer done()

	out := make([]Taxi10, tbl.NumRows())
	colInt64(tbl, "passenger_count", func(i int, v int64) { out[i].PassengerCount = v })
	colFloat64(tbl, "trip_distance", func(i int, v float64) { out[i].TripDistance = v })
	colInt64(tbl, "RatecodeID", func(i int, v int64) { out[i].RatecodeID = v })
	colInt32(tbl, "PULocationID", func(i int, v int32) { out[i].PULocationID = v })
	colInt32(tbl, "DOLocationID", func(i int, v int32) { out[i].DOLocationID = v })
	colInt64(tbl, "payment_type", func(i int, v int64) { out[i].PaymentType = v })
	colFloat64(tbl, "fare_amount", func(i int, v float64) { out[i].FareAmount = v })
	colFloat64(tbl, "tip_amount", func(i int, v float64) { out[i].TipAmount = v })
	colFloat64(tbl, "tolls_amount", func(i int, v float64) { out[i].TollsAmount = v })
	colFloat64(tbl, "total_amount", func(i int, v float64) { out[i].TotalAmount = v })

	return len(out)
}

func BenchmarkProj_ArrowGoRows(b *testing.B) {
	data := readFile(b)
	b.Run("01col", func(b *testing.B) { benchArrow(b, data, arrowRows1) })
	b.Run("05col", func(b *testing.B) { benchArrow(b, data, arrowRows5) })
	b.Run("10col", func(b *testing.B) { benchArrow(b, data, arrowRows10) })
}

func benchArrow(b *testing.B, data []byte, fn func(testing.TB, []byte) int) {
	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		total += fn(b, data)
	}

	reportRows(b, total)
}

// ── DuckDB via Go: query → []struct ───────────────────────────────────────────

func duckRows1(tb testing.TB, db *sql.DB) int {
	rows, err := db.Query("SELECT trip_distance FROM read_parquet('" + taxiPath() + "')")
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]Taxi1, 0, 3_000_000)

	var dist sql.NullFloat64

	for rows.Next() {
		if err := rows.Scan(&dist); err != nil {
			tb.Fatal(err)
		}

		out = append(out, Taxi1{TripDistance: dist.Float64})
	}

	return len(out)
}

func duckRows5(tb testing.TB, db *sql.DB) int {
	rows, err := db.Query("SELECT PULocationID,DOLocationID,trip_distance,fare_amount,total_amount FROM read_parquet('" + taxiPath() + "')")
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]Taxi5, 0, 3_000_000)

	var (
		pu, do            sql.NullInt32
		dist, fare, total sql.NullFloat64
	)

	for rows.Next() {
		if err := rows.Scan(&pu, &do, &dist, &fare, &total); err != nil {
			tb.Fatal(err)
		}

		out = append(out, Taxi5{
			PULocationID: pu.Int32, DOLocationID: do.Int32,
			TripDistance: dist.Float64, FareAmount: fare.Float64, TotalAmount: total.Float64,
		})
	}

	return len(out)
}

func duckRows10(tb testing.TB, db *sql.DB) int {
	rows, err := db.Query("SELECT passenger_count,trip_distance,RatecodeID,PULocationID,DOLocationID," +
		"payment_type,fare_amount,tip_amount,tolls_amount,total_amount FROM read_parquet('" + taxiPath() + "')")
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]Taxi10, 0, 3_000_000)

	var (
		pax, rate, pay              sql.NullInt64
		pu, do                      sql.NullInt32
		dist, fare, tip, tolls, tot sql.NullFloat64
	)

	for rows.Next() {
		if err := rows.Scan(&pax, &dist, &rate, &pu, &do, &pay, &fare, &tip, &tolls, &tot); err != nil {
			tb.Fatal(err)
		}

		out = append(out, Taxi10{
			PassengerCount: pax.Int64, TripDistance: dist.Float64, RatecodeID: rate.Int64,
			PULocationID: pu.Int32, DOLocationID: do.Int32, PaymentType: pay.Int64,
			FareAmount: fare.Float64, TipAmount: tip.Float64, TollsAmount: tolls.Float64, TotalAmount: tot.Float64,
		})
	}

	return len(out)
}

func BenchmarkProj_DuckDB(b *testing.B) {
	skipIfNoData(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	b.Run("01col", func(b *testing.B) { benchDuck(b, db, duckRows1) })
	b.Run("05col", func(b *testing.B) { benchDuck(b, db, duckRows5) })
	b.Run("10col", func(b *testing.B) { benchDuck(b, db, duckRows10) })
}

func benchDuck(b *testing.B, db *sql.DB, fn func(testing.TB, *sql.DB) int) {
	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		total += fn(b, db)
	}

	reportRows(b, total)
}
