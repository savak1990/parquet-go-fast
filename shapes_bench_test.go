package parquetfast_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Benchmark matrix across data-model shapes, comparing parquet-go's reflection
// GenericReader against parquet-go-fast — both STREAMING with a reused 4096-row
// buffer (apples-to-apples). All typed fast-path registrations are applied (see
// init below), so this measures the best-case parquet-go-fast path.
//
// Run: go test -run='^$' -bench=BenchmarkShape -benchmem -benchtime=3x
// (each shape uses 100k rows over 2 row groups, snappy-compressed.)

const shapeRows = 100_000

func buildFixture[T any](b *testing.B, n int, mk func(i int) T) []byte {
	b.Helper()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[T](&buf,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(50_000),
	)

	const batch = 4096

	rows := make([]T, batch)

	for written := 0; written < n; {
		k := min(batch, n-written)
		for i := 0; i < k; i++ {
			rows[i] = mk(written + i)
		}

		if _, err := w.Write(rows[:k]); err != nil {
			b.Fatalf("write: %v", err)
		}

		written += k
	}

	if err := w.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	return buf.Bytes()
}

func benchReaders[T any](b *testing.B, data []byte) {
	b.Helper()

	b.Run("parquet-go", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			r := parquet.NewGenericReader[T](bytes.NewReader(data))
			buf := make([]T, 4096)

			for {
				n, err := r.Read(buf)
				if n == 0 || err != nil {
					_ = r.Close()

					break
				}
			}
		}
	})

	b.Run("parquet-go-fast", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			rd, err := parquetfast.NewReader[T](bytes.NewReader(data), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}

			buf := make([]T, 4096)
			for {
				n, err := rd.Read(buf)
				if n == 0 || err != nil {
					break
				}
			}

			_ = rd.Close()
		}
	})
}

// ── Shape A: flat scalars only (simplest) ────────────────────────────────────

type flatShape struct {
	A int64   `parquet:"a"`
	B int64   `parquet:"b"`
	C float64 `parquet:"c"`
	D float64 `parquet:"d"`
	E string  `parquet:"e"`
	F string  `parquet:"f"`
	G bool    `parquet:"g"`
	H int32   `parquet:"h"`
	I int32   `parquet:"i"`
	J float64 `parquet:"j"`
}

func BenchmarkShapeA_FlatScalars(b *testing.B) {
	data := buildFixture(b, shapeRows, func(i int) flatShape {
		return flatShape{
			A: int64(i), B: int64(i * 3), C: float64(i) * 1.5, D: float64(i) * 0.25,
			E: fmt.Sprintf("name-%d", i), F: fmt.Sprintf("ns-%d", i%64),
			G: i%2 == 0, H: int32(i), I: int32(i % 1000), J: float64(i % 50),
		}
	})
	benchReaders[flatShape](b, data)
}

// ── Shape B: scalars + optionals + []byte blob ───────────────────────────────

type blobShape struct {
	TS   int64    `parquet:"ts"`
	Name string   `parquet:"name"`
	V1   float64  `parquet:"v1"`
	V2   float64  `parquet:"v2"`
	V3   float64  `parquet:"v3"`
	Opt1 *float64 `parquet:"opt1,optional"`
	Opt2 *int64   `parquet:"opt2,optional"`
	Hist []byte   `parquet:"hist"`
}

func BenchmarkShapeB_OptionalsBlob(b *testing.B) {
	data := buildFixture(b, shapeRows, func(i int) blobShape {
		r := blobShape{
			TS: int64(i), Name: fmt.Sprintf("m-%d", i),
			V1: float64(i) * 1.1, V2: float64(i) * 2.2, V3: float64(i) * 3.3,
			Hist: blob(i, 48),
		}
		if i%2 == 0 {
			f := float64(i) * 0.5
			r.Opt1 = &f
		}
		if i%3 == 0 {
			n := int64(i)
			r.Opt2 = &n
		}

		return r
	})
	benchReaders[blobShape](b, data)
}

// ── Shape C: primitive maps ──────────────────────────────────────────────────

type pmapShape struct {
	ID     int64             `parquet:"id"`
	Labels map[string]string `parquet:"labels"`
	Counts map[string]int64  `parquet:"counts"`
	Gauges map[int64]float64 `parquet:"gauges"`
}

func BenchmarkShapeC_PrimitiveMaps(b *testing.B) {
	data := buildFixture(b, shapeRows, func(i int) pmapShape {
		return pmapShape{
			ID:     int64(i),
			Labels: map[string]string{"app": fmt.Sprintf("a%d", i%50), "env": "prod", "tier": "web"},
			Counts: map[string]int64{"hits": int64(i), "miss": int64(i % 7)},
			Gauges: map[int64]float64{1: float64(i) * 0.1, 2: float64(i) * 0.2},
		}
	})
	benchReaders[pmapShape](b, data)
}

// ── Shape D: struct-valued map (high cardinality) ────────────────────────────

type cellShape struct {
	X int64   `parquet:"x"`
	Y float64 `parquet:"y"`
	N string  `parquet:"n"`
}

type smapShape struct {
	ID    int64                `parquet:"id"`
	Cells map[string]cellShape `parquet:"cells"`
}

func init() {
	parquetfast.RegisterStructValuedMap[string, cellShape](func(v parquet.Value) string {
		return string(v.ByteArray())
	})
	parquetfast.RegisterStructAlloc[d1Shape]()
	parquetfast.RegisterStructAlloc[d2Shape]()
	parquetfast.RegisterStructAlloc[d3Shape]()
	parquetfast.RegisterStructList[lineShape]()
}

func BenchmarkShapeD_StructValuedMap(b *testing.B) {
	data := buildFixture(b, shapeRows, func(i int) smapShape {
		cells := make(map[string]cellShape, 12)
		for j := 0; j < 12; j++ {
			cells[fmt.Sprintf("c%d", j)] = cellShape{X: int64(i + j), Y: float64(i) * 0.5, N: fmt.Sprintf("n%d", j)}
		}

		return smapShape{ID: int64(i), Cells: cells}
	})
	benchReaders[smapShape](b, data)
}

// ── Shape E: deep nested structs (required + optional) ────────────────────────

type d3Shape struct {
	V int64   `parquet:"v"`
	W float64 `parquet:"w"`
}

type d2Shape struct {
	Name  string   `parquet:"name"`
	Inner d3Shape  `parquet:"inner"`
	Opt   *d3Shape `parquet:"opt,optional"`
}

type d1Shape struct {
	Name string   `parquet:"name"`
	Mid  d2Shape  `parquet:"mid"`
	Opt  *d2Shape `parquet:"opt,optional"`
}

type deepShape struct {
	ID  int64    `parquet:"id"`
	Top d1Shape  `parquet:"top"`
	Opt *d1Shape `parquet:"opt,optional"`
}

func BenchmarkShapeE_DeepNested(b *testing.B) {
	data := buildFixture(b, shapeRows, func(i int) deepShape {
		r := deepShape{
			ID: int64(i),
			Top: d1Shape{
				Name: fmt.Sprintf("t%d", i),
				Mid:  d2Shape{Name: "m", Inner: d3Shape{V: int64(i), W: float64(i)}},
			},
		}
		if i%2 == 0 {
			r.Top.Mid.Opt = &d3Shape{V: int64(i * 2), W: float64(i) * 0.5}
		}
		if i%3 == 0 {
			r.Top.Opt = &d2Shape{Name: "mo", Inner: d3Shape{V: int64(i), W: 1}}
		}
		if i%4 == 0 {
			r.Opt = &d1Shape{Name: "to", Mid: d2Shape{Name: "x", Inner: d3Shape{V: 1, W: 2}}}
		}

		return r
	})
	benchReaders[deepShape](b, data)
}

// ── Shape F: struct slices ───────────────────────────────────────────────────

type lineShape struct {
	SKU   string  `parquet:"sku"`
	Qty   int64   `parquet:"qty"`
	Price float64 `parquet:"price"`
}

type sliceShape struct {
	ID    int64       `parquet:"id"`
	Items []lineShape `parquet:"items"`
}

func BenchmarkShapeF_StructSlices(b *testing.B) {
	data := buildFixture(b, shapeRows, func(i int) sliceShape {
		items := make([]lineShape, 8)
		for j := range items {
			items[j] = lineShape{SKU: fmt.Sprintf("s%d-%d", i, j), Qty: int64(j + 1), Price: float64(i) * 0.99}
		}

		return sliceShape{ID: int64(i), Items: items}
	})
	benchReaders[sliceShape](b, data)
}

// ── Shape G: time.Time heavy ─────────────────────────────────────────────────

type timeShape struct {
	ID      int64       `parquet:"id"`
	Created time.Time   `parquet:"created"`
	Updated time.Time   `parquet:"updated"`
	Deleted *time.Time  `parquet:"deleted,optional"`
	Events  []time.Time `parquet:"events"`
}

func BenchmarkShapeG_TimeHeavy(b *testing.B) {
	base := time.Unix(1_700_000_000, 0).UTC()
	data := buildFixture(b, shapeRows, func(i int) timeShape {
		r := timeShape{
			ID:      int64(i),
			Created: base.Add(time.Duration(i) * time.Second),
			Updated: base.Add(time.Duration(i*2) * time.Second),
			Events:  []time.Time{base.Add(time.Duration(i) * time.Minute), base.Add(time.Duration(i+1) * time.Minute)},
		}
		if i%2 == 0 {
			d := base.Add(time.Duration(i) * time.Hour)
			r.Deleted = &d
		}

		return r
	})
	benchReaders[timeShape](b, data)
}

// ── Shape H: wide mixed (reuses the merchant model — all features at once) ────

func BenchmarkShapeH_WideMixed(b *testing.B) {
	data := buildFixture(b, shapeRows, func(i int) merchantDay {
		return makeMerchant(i+1, i%6)
	})
	benchReaders[merchantDay](b, data)
}
