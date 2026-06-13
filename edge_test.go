package parquetfast_test

import (
	"math"
	"reflect"
	"testing"
)

// Edge-case hunt: shapes and values we expect to support, probed for correctness
// bugs (sign extension, full-range unsigned, optional zero-vs-nil, named types,
// deep nesting, float specials, non-string map keys, high cardinality).

// ── Narrow-int sign extension (full ranges incl. negatives) ──────────────────

type signedRow struct {
	I8  int8  `parquet:"i8"`
	I16 int16 `parquet:"i16"`
	I32 int32 `parquet:"i32"`
	I64 int64 `parquet:"i64"`
	I   int   `parquet:"i"`
}

func TestEdge_SignedRanges(t *testing.T) {
	rows := []signedRow{
		{I8: math.MinInt8, I16: math.MinInt16, I32: math.MinInt32, I64: math.MinInt64, I: math.MinInt32},
		{I8: math.MaxInt8, I16: math.MaxInt16, I32: math.MaxInt32, I64: math.MaxInt64, I: math.MaxInt32},
		{I8: -1, I16: -1, I32: -1, I64: -1, I: -1},
		{I8: 0, I16: 0, I32: 0, I64: 0, I: 0},
		{I8: -100, I16: -3000, I32: -70000, I64: -1 << 40, I: -123456},
	}

	roundtrip(t, rows)
}

// ── Full-range unsigned (incl. uint8 scalar, which is distinct from []byte) ───

type unsignedRow struct {
	U8  uint8  `parquet:"u8"`
	U16 uint16 `parquet:"u16"`
	U32 uint32 `parquet:"u32"`
	U64 uint64 `parquet:"u64"`
}

func TestEdge_UnsignedRanges(t *testing.T) {
	rows := []unsignedRow{
		{U8: 0, U16: 0, U32: 0, U64: 0},
		{U8: math.MaxUint8, U16: math.MaxUint16, U32: math.MaxUint32, U64: math.MaxUint64},
		{U8: 128, U16: 32768, U32: 2147483648, U64: 1 << 63},
		{U8: 200, U16: 60000, U32: 4000000000, U64: 18000000000000000000},
	}

	roundtrip(t, rows)
}

// ── Optional zero-value vs nil (present-with-zero must differ from absent) ────

type optZeroRow struct {
	I *int64   `parquet:"i,optional"`
	S *string  `parquet:"s,optional"`
	B *bool    `parquet:"b,optional"`
	F *float64 `parquet:"f,optional"`
}

func TestEdge_OptionalZeroVsNil(t *testing.T) {
	z := int64(0)
	es := ""
	bf := false
	zf := 0.0

	rows := []optZeroRow{
		{I: &z, S: &es, B: &bf, F: &zf}, // all present, all zero
		{},                              // all absent
	}

	roundtrip(t, rows)
}

// ── Named scalar types (type aliases over primitives) ────────────────────────

type Currency string
type Cents int64
type Rating uint8
type Score float64

type namedRow struct {
	Cur   Currency   `parquet:"cur"`
	Total Cents      `parquet:"total"`
	Stars Rating     `parquet:"stars"`
	Score Score      `parquet:"score"`
	Tags  []Currency `parquet:"tags"`
}

func TestEdge_NamedTypes(t *testing.T) {
	rows := []namedRow{
		{Cur: "USD", Total: 1999, Stars: 5, Score: 9.5, Tags: []Currency{"USD", "EUR"}},
		{Cur: "JPY", Total: -50, Stars: 0, Score: -1.25, Tags: []Currency{"JPY"}},
	}

	roundtrip(t, rows)
}

// ── Named map key/value types ────────────────────────────────────────────────

type Region string
type MetricName string

type namedMapRow struct {
	ByRegion map[Region]int64       `parquet:"by_region"`
	Metrics  map[MetricName]float64 `parquet:"metrics"`
}

func TestEdge_NamedMapKeys(t *testing.T) {
	rows := []namedMapRow{
		{
			ByRegion: map[Region]int64{"us": 1, "eu": 2},
			Metrics:  map[MetricName]float64{"p50": 1.5, "p99": 9.9},
		},
		{
			ByRegion: map[Region]int64{"apac": 3},
			Metrics:  map[MetricName]float64{"p50": 2.0},
		},
	}

	roundtrip(t, rows)
}

// ── Deep required + optional nesting (3 levels) ──────────────────────────────

type level3 struct {
	V int64 `parquet:"v"`
}

type level2 struct {
	Name  string  `parquet:"name"`
	L3    level3  `parquet:"l3"`
	OptL3 *level3 `parquet:"opt_l3,optional"`
}

type level1 struct {
	Name  string  `parquet:"name"`
	L2    level2  `parquet:"l2"`
	OptL2 *level2 `parquet:"opt_l2,optional"`
}

type deepRow struct {
	ID string `parquet:"id"`
	L1 level1 `parquet:"l1"`
}

func TestEdge_DeepNesting(t *testing.T) {
	rows := []deepRow{
		{ID: "a", L1: level1{
			Name:  "l1a",
			L2:    level2{Name: "l2a", L3: level3{V: 1}, OptL3: &level3{V: 2}},
			OptL2: &level2{Name: "opt2a", L3: level3{V: 3}},
		}},
		{ID: "b", L1: level1{
			Name: "l1b",
			L2:   level2{Name: "l2b", L3: level3{V: 4}}, // OptL3 nil
			// OptL2 nil
		}},
	}

	roundtrip(t, rows)
}

// ── Float specials (Inf, -0.0) — NaN excluded (NaN != NaN) ───────────────────

type floatRow struct {
	A float64 `parquet:"a"`
	B float32 `parquet:"b"`
}

func TestEdge_FloatSpecials(t *testing.T) {
	rows := []floatRow{
		{A: math.Inf(1), B: float32(math.Inf(1))},
		{A: math.Inf(-1), B: float32(math.Inf(-1))},
		{A: math.SmallestNonzeroFloat64, B: math.SmallestNonzeroFloat32},
		{A: math.MaxFloat64, B: math.MaxFloat32},
		{A: math.Copysign(0, -1), B: float32(math.Copysign(0, -1))}, // -0.0
	}

	roundtrip(t, rows)
}

// ── Non-string primitive map keys ────────────────────────────────────────────

type keyKindsRow struct {
	I32K map[int32]string  `parquet:"i32k"`
	I64K map[int64]string  `parquet:"i64k"`
	F64K map[float64]int64 `parquet:"f64k"`
}

func TestEdge_MapKeyKinds(t *testing.T) {
	rows := []keyKindsRow{
		{
			I32K: map[int32]string{1: "a", -5: "b"},
			I64K: map[int64]string{1 << 40: "big"},
			F64K: map[float64]int64{1.5: 10, 2.5: 20},
		},
		{
			I32K: map[int32]string{0: "zero"},
			I64K: map[int64]string{-1: "neg"},
			F64K: map[float64]int64{3.14: 30},
		},
	}

	roundtrip(t, rows)
}

// ── int64-keyed struct-valued map (reflect fallback) ─────────────────────────

type throttle struct {
	Limit int64 `parquet:"limit"`
	Burst int64 `parquet:"burst"`
}

type int64MapRow struct {
	Throttles map[int64]throttle `parquet:"throttles"`
}

func TestEdge_Int64KeyedStructMap(t *testing.T) {
	rows := []int64MapRow{
		{Throttles: map[int64]throttle{1: {Limit: 10, Burst: 20}, 2: {Limit: 30, Burst: 40}}},
		{Throttles: map[int64]throttle{100: {Limit: 1, Burst: 2}}},
	}

	roundtrip(t, rows)
}

// ── High-cardinality map and list in a single row ────────────────────────────

type bigCardRow struct {
	Labels map[string]int64 `parquet:"labels"`
	Items  []int64          `parquet:"items"`
}

func TestEdge_HighCardinality(t *testing.T) {
	labels := make(map[string]int64, 2000)
	items := make([]int64, 0, 5000)

	for i := 0; i < 2000; i++ {
		labels[stringKey(i)] = int64(i)
	}

	for i := 0; i < 5000; i++ {
		items = append(items, int64(i))
	}

	roundtrip(t, []bigCardRow{{Labels: labels, Items: items}})
}

func stringKey(i int) string {
	return "k" + string(rune('A'+i%26)) + string(rune('0'+(i/26)%10)) + string(rune('a'+(i/260)%26)) + string(rune('0'+i/6760))
}

// ── Empty/edge strings and unicode ───────────────────────────────────────────

type strRow struct {
	A string   `parquet:"a"`
	B string   `parquet:"b"`
	L []string `parquet:"l"`
}

func TestEdge_Strings(t *testing.T) {
	rows := []strRow{
		{A: "", B: "with\x00null", L: []string{"", "x", "café", "日本語", "emoji😀"}},
		{A: "café", B: "日本語", L: []string{"line\nbreak", "tab\there"}},
	}

	roundtrip(t, rows)
}

// sanity: reflect.DeepEqual treats -0.0 == 0.0; make the intent explicit.
func TestEdge_NegZeroDeepEqualSanity(t *testing.T) {
	if !reflect.DeepEqual(math.Copysign(0, -1), 0.0) {
		t.Skip("DeepEqual distinguishes -0.0; float special test would need bit compare")
	}
}
