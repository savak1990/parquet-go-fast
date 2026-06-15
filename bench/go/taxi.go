// Package bench holds cross-technology read benchmarks for parquet-go-fast.
//
// The dataset is the public NYC TLC yellow-taxi trip records (see ../README.md).
// Workloads mirror the standard parquet-reader benchmark cases: full row
// materialization, column projection, and predicate-filtered scans. The Go
// benchmarks here compare this library against parquet-go's reflection-based
// GenericReader; the engine comparisons (DuckDB / ClickHouse) live in ../sql.
package bench

import (
	"os"
	"time"
)

// Taxi is the full 19-column NYC yellow-taxi record. All columns are physically
// optional in the file; value-typed fields decode null as the Go zero value,
// which both readers do identically (a fair apples-to-apples mapping).
type Taxi struct {
	VendorID             int32     `parquet:"VendorID"`
	PickupTime           time.Time `parquet:"tpep_pickup_datetime"`
	DropoffTime          time.Time `parquet:"tpep_dropoff_datetime"`
	PassengerCount       int64     `parquet:"passenger_count"`
	TripDistance         float64   `parquet:"trip_distance"`
	RatecodeID           int64     `parquet:"RatecodeID"`
	StoreAndFwdFlag      string    `parquet:"store_and_fwd_flag"`
	PULocationID         int32     `parquet:"PULocationID"`
	DOLocationID         int32     `parquet:"DOLocationID"`
	PaymentType          int64     `parquet:"payment_type"`
	FareAmount           float64   `parquet:"fare_amount"`
	Extra                float64   `parquet:"extra"`
	MTATax               float64   `parquet:"mta_tax"`
	TipAmount            float64   `parquet:"tip_amount"`
	TollsAmount          float64   `parquet:"tolls_amount"`
	ImprovementSurcharge float64   `parquet:"improvement_surcharge"`
	TotalAmount          float64   `parquet:"total_amount"`
	CongestionSurcharge  float64   `parquet:"congestion_surcharge"`
	AirportFee           float64   `parquet:"Airport_fee"`
}

// TaxiProjection is the 4-column subset used for the projection workload: a wide
// file read through a narrow struct.
type TaxiProjection struct {
	PULocationID int32   `parquet:"PULocationID"`
	TripDistance float64 `parquet:"trip_distance"`
	FareAmount   float64 `parquet:"fare_amount"`
	TotalAmount  float64 `parquet:"total_amount"`
}

func taxiPath() string {
	if p := os.Getenv("TAXI_FILE"); p != "" {
		return p
	}

	return "../data/yellow_tripdata_2024-01.parquet"
}
