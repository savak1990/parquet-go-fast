package bench

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// DuckDB-via-Go comparison. Unlike an analytical `SELECT sum(...)` (which returns
// a scalar and never materializes rows), these queries materialize the full
// result set into a ready-to-use Go slice — the same end state our library and
// parquet-go produce — so the comparison is within-category and fair. The cost
// measured is end to end: query submission, DuckDB's parquet scan/decode, the
// database/sql driver, and Scan into Go structs (with NULL→zero conversion to
// match the value-typed Taxi struct).
//
// DuckDB reads from the file path (OS page cache, warm after the first read);
// the Go decoders read from an in-memory copy. Both are warm/decode-bound.

const taxiCols = `VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,` +
	`trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,` +
	`fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,` +
	`congestion_surcharge,Airport_fee`

func openDuckDB(tb testing.TB) *sql.DB {
	tb.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		tb.Fatalf("open duckdb: %v", err)
	}

	return db
}

func duckdbFull(tb testing.TB, db *sql.DB, path string) []Taxi {
	tb.Helper()

	rows, err := db.Query("SELECT " + taxiCols + " FROM read_parquet('" + path + "')")
	if err != nil {
		tb.Fatalf("duckdb query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var (
		out []Taxi

		vendor                                                    sql.NullInt32
		pickup, dropoff                                           sql.NullTime
		pax, rate, pay                                            sql.NullInt64
		flag                                                      sql.NullString
		pu, do                                                    sql.NullInt32
		dist, fare, extra, mta, tip, tolls, imp, total, cong, air sql.NullFloat64
	)

	for rows.Next() {
		if err := rows.Scan(&vendor, &pickup, &dropoff, &pax, &dist, &rate, &flag, &pu, &do,
			&pay, &fare, &extra, &mta, &tip, &tolls, &imp, &total, &cong, &air); err != nil {
			tb.Fatalf("scan: %v", err)
		}

		out = append(out, Taxi{
			VendorID: vendor.Int32, PickupTime: pickup.Time, DropoffTime: dropoff.Time,
			PassengerCount: pax.Int64, TripDistance: dist.Float64, RatecodeID: rate.Int64,
			StoreAndFwdFlag: flag.String, PULocationID: pu.Int32, DOLocationID: do.Int32,
			PaymentType: pay.Int64, FareAmount: fare.Float64, Extra: extra.Float64,
			MTATax: mta.Float64, TipAmount: tip.Float64, TollsAmount: tolls.Float64,
			ImprovementSurcharge: imp.Float64, TotalAmount: total.Float64,
			CongestionSurcharge: cong.Float64, AirportFee: air.Float64,
		})
	}

	if err := rows.Err(); err != nil {
		tb.Fatalf("rows: %v", err)
	}

	return out
}

func duckdbProjection(tb testing.TB, db *sql.DB, where string) []TaxiProjection {
	tb.Helper()

	q := "SELECT PULocationID,trip_distance,fare_amount,total_amount FROM read_parquet('" + taxiPath() + "')"
	if where != "" {
		q += " WHERE " + where
	}

	rows, err := db.Query(q)
	if err != nil {
		tb.Fatalf("duckdb query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var (
		out               []TaxiProjection
		pu                sql.NullInt32
		dist, fare, total sql.NullFloat64
	)

	for rows.Next() {
		if err := rows.Scan(&pu, &dist, &fare, &total); err != nil {
			tb.Fatalf("scan: %v", err)
		}

		out = append(out, TaxiProjection{
			PULocationID: pu.Int32, TripDistance: dist.Float64,
			FareAmount: fare.Float64, TotalAmount: total.Float64,
		})
	}

	if err := rows.Err(); err != nil {
		tb.Fatalf("rows: %v", err)
	}

	return out
}

func skipIfNoData(b *testing.B) {
	if _, err := os.Stat(taxiPath()); err != nil {
		b.Skipf("benchmark data missing (%v); run bench/download.sh", err)
	}
}

func BenchmarkFull_DuckDB(b *testing.B) {
	skipIfNoData(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for b.Loop() {
		total += len(duckdbFull(b, db, taxiPath()))
	}

	reportRows(b, total)
}

func BenchmarkFilter_DuckDB(b *testing.B) {
	skipIfNoData(b)

	db := openDuckDB(b)
	defer func() { _ = db.Close() }()

	for _, fc := range filterCases {
		b.Run(fc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			var matched int
			for b.Loop() {
				matched = len(duckdbProjection(b, db, fc.sql))
			}

			b.ReportMetric(float64(matched), "matched")
		})
	}
}
