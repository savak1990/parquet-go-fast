package parquetfast_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// This file exercises the decoder against records that are deliberately UNLIKE
// any production data in domain (e-commerce / logistics analytics) but match the
// structural complexity of a real columnar rollup: a wide top-level record with
// scalars (incl. narrow ints), optional primitives, optional []byte blobs, a
// nested optional struct chain, a high-cardinality struct-valued map, a nested
// map, a struct slice, primitive slices, and several opaque []byte histogram
// columns. Two top-level types stand in for a two-type production schema.
//
// All data is generated deterministically from a seed; collections are authored
// non-empty-or-nil so plain reflect.DeepEqual holds (parquet can't distinguish a
// nil slice/map from an empty one for required fields).

// ── Domain type 1: a merchant's daily sales rollup ───────────────────────────

type extraStats struct {
	P50 float64 `parquet:"p50"`
	P99 float64 `parquet:"p99"`
}

type dailyTotals struct {
	Sessions int64       `parquet:"sessions"`
	Bounce   float64     `parquet:"bounce"`
	Extra    *extraStats `parquet:"extra,optional"` // nested optional struct
}

type priceStats struct {
	MinCents  int64   `parquet:"min_cents"`
	MaxCents  int64   `parquet:"max_cents"`
	MeanCents float64 `parquet:"mean_cents"`
	StdDev    float64 `parquet:"std_dev"`
	Histogram []byte  `parquet:"histogram"` // opaque blob, like a serialized t-digest
}

type product struct {
	SKU       string      `parquet:"sku"`
	Title     string      `parquet:"title"`
	UnitsSold int64       `parquet:"units_sold"`
	Returns   int32       `parquet:"returns"`
	Rating    int8        `parquet:"rating"` // narrow int (1..5)
	InStock   bool        `parquet:"in_stock"`
	Revenue   float64     `parquet:"revenue"`
	Tags      []string    `parquet:"tags"`
	Stats     *priceStats `parquet:"stats,optional"`
}

type campaign struct {
	Name     string  `parquet:"name"`
	Discount float64 `parquet:"discount"`
	Redeemed int64   `parquet:"redeemed"`
}

type merchantDay struct {
	MerchantID   string                        `parquet:"merchant_id"`
	Region       string                        `parquet:"region"`
	Category     string                        `parquet:"category"`
	Date         int64                         `parquet:"date"`
	Currency     string                        `parquet:"currency"`
	OrderCount   int64                         `parquet:"order_count"`
	RefundCount  int32                         `parquet:"refund_count"`
	Tier         uint8                         `parquet:"tier"` // narrow uint
	AvgOrder     float64                       `parquet:"avg_order"`
	GrossCents   int64                         `parquet:"gross_cents"`
	Verified     bool                          `parquet:"verified"`
	Note         *string                       `parquet:"note,optional"`        // optional primitive
	PeakHour     *int64                        `parquet:"peak_hour,optional"`   // optional primitive
	DailyHisto   *[]byte                       `parquet:"daily_histo,optional"` // optional blob
	PaymentTypes []string                      `parquet:"payment_types"`
	Labels       map[string]string             `parquet:"labels"`
	Products     map[string]product            `parquet:"products"` // high-cardinality struct map
	Funnel       map[string]map[string]float64 `parquet:"funnel"`   // nested map
	Campaigns    []campaign                    `parquet:"campaigns"`
	Daily        *dailyTotals                  `parquet:"daily,optional"`
}

// ── Domain type 2: a warehouse's daily ops rollup (blob-heavy, like a node
// rollup with many distribution columns) ─────────────────────────────────────

type warehouseStats struct {
	Throughput []byte `parquet:"throughput"`
	Backlog    []byte `parquet:"backlog"`
	Staffing   int32  `parquet:"staffing"`
}

type warehouseDay struct {
	WarehouseID string            `parquet:"warehouse_id"`
	Region      string            `parquet:"region"`
	Date        int64             `parquet:"date"`
	Inbound     int64             `parquet:"inbound"`
	Outbound    int64             `parquet:"outbound"`
	CapacityPct float64           `parquet:"capacity_pct"`
	Zone        uint16            `parquet:"zone"` // narrow uint
	PickLatency []byte            `parquet:"pick_latency"`
	PackLatency []byte            `parquet:"pack_latency"`
	ShipLatency []byte            `parquet:"ship_latency"`
	DwellTime   []byte            `parquet:"dwell_time"`
	Utilization []byte            `parquet:"utilization"`
	NodeLabels  map[string]string `parquet:"node_labels"`
	Stats       *warehouseStats   `parquet:"stats,optional"`
}

func init() {
	parquetfast.RegisterStructAlloc[priceStats]()
	parquetfast.RegisterStructAlloc[dailyTotals]()
	parquetfast.RegisterStructAlloc[extraStats]()
	parquetfast.RegisterStructAlloc[warehouseStats]()
	parquetfast.RegisterStructList[campaign]()
	parquetfast.RegisterStructValuedMap[string, product](func(v parquet.Value) string {
		return string(v.ByteArray())
	})
}

// ── Deterministic generators ─────────────────────────────────────────────────

// blob produces a deterministic, non-empty byte payload for seed.
func blob(seed, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((seed*31 + i*7 + 13) % 251)
	}

	return b
}

func makeProduct(seed int) product {
	p := product{
		SKU:       fmt.Sprintf("SKU-%06d", seed),
		Title:     fmt.Sprintf("Product %d", seed),
		UnitsSold: int64(seed%500) + 1,
		Returns:   int32(seed % 7),
		Rating:    int8(seed%5) + 1,
		InStock:   seed%3 != 0,
		Revenue:   float64(seed%9999) * 1.07,
		Tags:      []string{fmt.Sprintf("c%d", seed%12), "online"},
	}

	// Every 4th product carries an optional price-stats struct with a blob.
	if seed%4 == 0 {
		p.Stats = &priceStats{
			MinCents:  int64(seed % 100),
			MaxCents:  int64(seed%100) + 5000,
			MeanCents: float64(seed%5000) + 0.5,
			StdDev:    float64(seed%37) * 1.25,
			Histogram: blob(seed, 24),
		}
	}

	return p
}

// makeMerchant builds a merchant rollup whose nesting cardinality varies with
// seed: nProducts products, present/absent optionals, populated/nil collections.
func makeMerchant(seed, nProducts int) merchantDay {
	m := merchantDay{
		MerchantID:   fmt.Sprintf("merch-%08d", seed),
		Region:       []string{"us-east", "us-west", "eu", "apac"}[seed%4],
		Category:     []string{"apparel", "grocery", "electronics", "home"}[seed%4],
		Date:         1_700_000_000 + int64(seed)*86400,
		Currency:     "USD",
		OrderCount:   int64(seed%10000) + 1,
		RefundCount:  int32(seed % 50),
		Tier:         uint8(seed % 4),
		AvgOrder:     float64(seed%300) * 1.33,
		GrossCents:   int64(seed) * 999,
		Verified:     seed%2 == 0,
		PaymentTypes: []string{"card", "wallet"},
		Labels: map[string]string{
			"plan":  fmt.Sprintf("p%d", seed%5),
			"owner": fmt.Sprintf("u%d", seed%97),
		},
		Funnel: map[string]map[string]float64{
			"visit": {"rate": float64(seed%100) / 100.0, "count": float64(seed % 1000)},
			"cart":  {"rate": float64(seed%80) / 100.0},
			"buy":   {"rate": float64(seed%30) / 100.0},
		},
	}

	// Optional primitives: present on some rows.
	if seed%3 == 0 {
		note := fmt.Sprintf("note-%d", seed)
		m.Note = &note
	}

	if seed%5 == 0 {
		ph := int64(seed % 24)
		m.PeakHour = &ph
	}

	if seed%6 == 0 {
		b := blob(seed+1, 16)
		m.DailyHisto = &b
	}

	// High-cardinality struct-valued map.
	if nProducts > 0 {
		m.Products = make(map[string]product, nProducts)
		for i := 0; i < nProducts; i++ {
			p := makeProduct(seed*100 + i)
			m.Products[p.SKU] = p
		}
	}

	// Struct slice on some rows.
	if seed%2 == 0 {
		m.Campaigns = []campaign{
			{Name: "spring", Discount: 0.1, Redeemed: int64(seed % 200)},
			{Name: "loyalty", Discount: 0.05, Redeemed: int64(seed % 50)},
		}
	}

	// Nested optional struct chain on some rows.
	if seed%3 == 0 {
		d := &dailyTotals{Sessions: int64(seed % 9999), Bounce: float64(seed%100) / 100.0}
		if seed%9 == 0 {
			d.Extra = &extraStats{P50: float64(seed % 50), P99: float64(seed%50) + 49}
		}

		m.Daily = d
	}

	return m
}

func makeWarehouse(seed int) warehouseDay {
	w := warehouseDay{
		WarehouseID: fmt.Sprintf("wh-%06d", seed),
		Region:      []string{"us-east", "us-west", "eu", "apac"}[seed%4],
		Date:        1_700_000_000 + int64(seed)*86400,
		Inbound:     int64(seed%5000) + 1,
		Outbound:    int64(seed%4800) + 1,
		CapacityPct: float64(seed%100) / 100.0,
		Zone:        uint16(seed % 900),
		PickLatency: blob(seed, 20),
		PackLatency: blob(seed+1, 20),
		ShipLatency: blob(seed+2, 20),
		DwellTime:   blob(seed+3, 20),
		Utilization: blob(seed+4, 20),
		NodeLabels: map[string]string{
			"rack": fmt.Sprintf("r%d", seed%40),
			"team": fmt.Sprintf("t%d", seed%8),
		},
	}

	if seed%2 == 0 {
		w.Stats = &warehouseStats{
			Throughput: blob(seed+5, 32),
			Backlog:    blob(seed+6, 32),
			Staffing:   int32(seed % 200),
		}
	}

	return w
}

// ── Roundtrip correctness ────────────────────────────────────────────────────

func TestMerchantRoundtrip(t *testing.T) {
	rows := []merchantDay{
		makeMerchant(1, 0),  // no products, varied optionals
		makeMerchant(2, 1),  // one product
		makeMerchant(3, 8),  // several products
		makeMerchant(4, 25), // high cardinality
		makeMerchant(7, 3),
		makeMerchant(12, 0),
	}

	roundtrip(t, rows)
}

func TestWarehouseRoundtrip(t *testing.T) {
	rows := []warehouseDay{
		makeWarehouse(1),
		makeWarehouse(2),
		makeWarehouse(3),
		makeWarehouse(10),
	}

	roundtrip(t, rows)
}

// TestMerchantMultiRowGroupRoundtrip writes enough rows to force several row
// groups and verifies a full, exact decode across the group boundary.
func TestMerchantMultiRowGroupRoundtrip(t *testing.T) {
	const n = 300

	rows := make([]merchantDay, n)
	for i := range rows {
		rows[i] = makeMerchant(i+1, i%6) // cardinality 0..5, all shapes exercised
	}

	buf := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(64))

	got, err := parquetfast.UnmarshalBytes[merchantDay](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	if !reflect.DeepEqual(rows, got) {
		// Pinpoint the first differing row to keep failures readable.
		for i := range rows {
			if !reflect.DeepEqual(rows[i], got[i]) {
				t.Fatalf("row %d mismatch:\n want %#v\n got  %#v", i, rows[i], got[i])
			}
		}

		t.Fatal("decoded slice differs")
	}
}
