package parquetfast_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// These tests close coverage gaps for the value-type breadth of the library:
// every primitive-slice element kind, every optional scalar kind, every filter
// value type, and each decode Option.

// ── every primitive slice element kind (appendPrimitiveSlice) ─────────────────

type allSlicesRow struct {
	I   []int     `parquet:"i"`
	I8  []int8    `parquet:"i8"`
	I16 []int16   `parquet:"i16"`
	I32 []int32   `parquet:"i32"`
	I64 []int64   `parquet:"i64"`
	U16 []uint16  `parquet:"u16"`
	U32 []uint32  `parquet:"u32"`
	U64 []uint64  `parquet:"u64"`
	F32 []float32 `parquet:"f32"`
	F64 []float64 `parquet:"f64"`
	B   []bool    `parquet:"b"`
	S   []string  `parquet:"s"`
}

func TestCoverage_AllPrimitiveSlices(t *testing.T) {
	t.Parallel()

	rows := []allSlicesRow{
		{
			I: []int{1, -2}, I8: []int8{-1, 2}, I16: []int16{300, -300}, I32: []int32{1 << 20, -1},
			I64: []int64{1 << 40}, U16: []uint16{65535}, U32: []uint32{1 << 31}, U64: []uint64{1 << 63},
			F32: []float32{1.5, -2.5}, F64: []float64{3.25}, B: []bool{true, false}, S: []string{"a", "b"},
		},
		{
			I: []int{9}, I8: []int8{127}, I16: []int16{-1}, I32: []int32{7}, I64: []int64{-9},
			U16: []uint16{1}, U32: []uint32{2}, U64: []uint64{3}, F32: []float32{0.5}, F64: []float64{9.9},
			B: []bool{false}, S: []string{"x"},
		},
	}

	roundtrip(t, rows)
}

// ── every optional scalar kind (optionalKindFor + applyScalar opt arms) ────────

type allOptRow struct {
	I   *int     `parquet:"i,optional"`
	I8  *int8    `parquet:"i8,optional"`
	I16 *int16   `parquet:"i16,optional"`
	I32 *int32   `parquet:"i32,optional"`
	I64 *int64   `parquet:"i64,optional"`
	U8  *uint8   `parquet:"u8,optional"`
	U16 *uint16  `parquet:"u16,optional"`
	U32 *uint32  `parquet:"u32,optional"`
	U64 *uint64  `parquet:"u64,optional"`
	F32 *float32 `parquet:"f32,optional"`
	F64 *float64 `parquet:"f64,optional"`
	B   *bool    `parquet:"b,optional"`
	S   *string  `parquet:"s,optional"`
	Bs  *[]byte  `parquet:"bs,optional"`
}

func TestCoverage_AllOptionalKinds(t *testing.T) {
	t.Parallel()

	i, i8, i16, i32, i64 := 1, int8(-2), int16(3), int32(-4), int64(5)
	u8, u16, u32, u64 := uint8(6), uint16(7), uint32(8), uint64(9)
	f32, f64 := float32(1.5), 2.5
	b, s, bs := true, "x", []byte{1, 2}

	rows := []allOptRow{
		{I: &i, I8: &i8, I16: &i16, I32: &i32, I64: &i64, U8: &u8, U16: &u16, U32: &u32, U64: &u64, F32: &f32, F64: &f64, B: &b, S: &s, Bs: &bs},
		{}, // all nil
	}

	roundtrip(t, rows)
}

// ── every filter value type (toValue / asInt64 / asFloat / asBytes / op arms) ──

type valuesRow struct {
	ID  int64   `parquet:"id"`
	B   bool    `parquet:"b"`
	I   int     `parquet:"i"`
	I8  int8    `parquet:"i8"`
	I16 int16   `parquet:"i16"`
	I32 int32   `parquet:"i32"`
	U32 uint32  `parquet:"u32"`
	U64 uint64  `parquet:"u64"`
	F32 float32 `parquet:"f32"`
	F64 float64 `parquet:"f64"`
	S   string  `parquet:"s"`
	Bs  []byte  `parquet:"bs"`
}

func TestCoverage_FilterValueTypes(t *testing.T) {
	t.Parallel()

	rows := make([]valuesRow, 6)
	for i := range rows {
		rows[i] = valuesRow{
			ID: int64(i), B: i%2 == 0, I: i, I8: int8(i), I16: int16(i), I32: int32(i),
			U32: uint32(i), U64: uint64(i), F32: float32(i), F64: float64(i),
			S: string(rune('a' + i)), Bs: []byte{byte(i)},
		}
	}

	data := writeGeneric(t, rows)

	count := func(pred parquetfast.Predicate) int {
		got, err := parquetfast.UnmarshalBytes[valuesRow](data, parquetfast.Where(pred))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		return len(got)
	}

	// One predicate per value type / converter arm.
	if n := count(parquetfast.Col("b").Equal(true)); n != 3 {
		t.Fatalf("bool eq: %d", n)
	}

	if n := count(parquetfast.Col("i").Greater(int(3))); n != 2 {
		t.Fatalf("int gt: %d", n)
	}

	if n := count(parquetfast.Col("i8").Equal(int8(2))); n != 1 {
		t.Fatalf("int8 eq: %d", n)
	}

	if n := count(parquetfast.Col("i16").LessOrEqual(int16(1))); n != 2 {
		t.Fatalf("int16 le: %d", n)
	}

	if n := count(parquetfast.Col("i32").Between(int32(2), int32(4))); n != 3 {
		t.Fatalf("int32 between: %d", n)
	}

	if n := count(parquetfast.Col("u32").GreaterOrEqual(uint32(4))); n != 2 {
		t.Fatalf("uint32 ge: %d", n)
	}

	if n := count(parquetfast.Col("u64").Less(uint64(2))); n != 2 {
		t.Fatalf("uint64 lt: %d", n)
	}

	// uint / uint8 / uint16 predicate values against an int column (asInt64 arms).
	if n := count(parquetfast.Col("id").Equal(uint(3))); n != 1 {
		t.Fatalf("uint eq: %d", n)
	}

	if n := count(parquetfast.Col("id").Equal(uint8(4))); n != 1 {
		t.Fatalf("uint8 eq: %d", n)
	}

	if n := count(parquetfast.Col("id").Equal(uint16(5))); n != 1 {
		t.Fatalf("uint16 eq: %d", n)
	}

	if n := count(parquetfast.Col("f32").Greater(float32(3.0))); n != 2 {
		t.Fatalf("float32 gt: %d", n)
	}

	if n := count(parquetfast.Col("f64").Between(float64(1.0), float64(3.0))); n != 3 {
		t.Fatalf("float64 between: %d", n)
	}

	// int value against a float column exercises asFloat's integer fallback.
	if n := count(parquetfast.Col("f64").GreaterOrEqual(int(4))); n != 2 {
		t.Fatalf("float64 ge int: %d", n)
	}

	if n := count(parquetfast.Col("s").Equal("c")); n != 1 {
		t.Fatalf("string eq: %d", n)
	}

	if n := count(parquetfast.Col("bs").Equal([]byte{2})); n != 1 {
		t.Fatalf("bytes eq: %d", n)
	}
}

func TestCoverage_FilterTypeMismatchErrors(t *testing.T) {
	t.Parallel()

	data := writeGeneric(t, []valuesRow{{ID: 1}})

	// Wrong predicate value type for the column → compile error surfaced.
	if _, err := parquetfast.UnmarshalBytes[valuesRow](data, parquetfast.Where(parquetfast.Col("i").Equal("not-an-int"))); err == nil {
		t.Fatal("expected error for string predicate on int column")
	}

	// Unknown column → error.
	if _, err := parquetfast.UnmarshalBytes[valuesRow](data, parquetfast.Where(parquetfast.Col("nope").Equal(int64(1)))); err == nil {
		t.Fatal("expected error for unknown filter column")
	}
}

// ── every decode Option ───────────────────────────────────────────────────────

func TestCoverage_Options(t *testing.T) {
	t.Parallel()

	rows := make([]scalarRow, 200)
	for i := range rows {
		rows[i] = scalarRow{S: "r", I64: int64(i), Bs: []byte{byte(i)}}
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(40))
	size := int64(len(data))

	opts := [][]parquetfast.Option{
		{parquetfast.WithBatchSize(8)},
		{parquetfast.WithBatchSize(0)}, // clamped to default
		{parquetfast.WithoutNullColumnSkip()},
		{parquetfast.WithoutColumnProjection()},
		{parquetfast.WithConcurrency(3)},
		{parquetfast.WithReadBufferSize(1 << 16)},
		{parquetfast.WithOptimisticRead()},
		{parquetfast.WithAsyncReads()},
		{parquetfast.WithFileOptions(parquet.SkipBloomFilters(true))},
	}

	for i, o := range opts {
		got, err := parquetfast.Unmarshal[scalarRow](bytes.NewReader(data), size, o...)
		if err != nil {
			t.Fatalf("opt set %d: %v", i, err)
		}

		if len(got) != len(rows) {
			t.Fatalf("opt set %d: got %d rows, want %d", i, len(got), len(rows))
		}
	}

	// ReaderAtFunc + a Reader option, exercised through NewReader.
	ra := parquetfast.ReaderAtFunc(func(p []byte, off int64) (int, error) {
		return bytes.NewReader(data).ReadAt(p, off)
	})

	rd, err := parquetfast.NewReader[scalarRow](ra, size, parquetfast.WithReadBufferSize(1<<15))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	defer func() { _ = rd.Close() }()

	if rd.NumRows() != int64(len(rows)) {
		t.Fatalf("NumRows: %d", rd.NumRows())
	}

	if !reflect.DeepEqual(rd.Schema().Columns(), rd.File().Schema().Columns()) {
		t.Fatal("Schema()/File() mismatch")
	}
}

// ── time predicate against every logical unit (timeToValue) ───────────────────

func TestCoverage_FilterTimeUnits(t *testing.T) {
	t.Parallel()

	type tRow struct {
		Millis time.Time `parquet:"millis,timestamp(millisecond)"`
		Micros time.Time `parquet:"micros,timestamp(microsecond)"`
		Nanos  time.Time `parquet:"nanos"` // default: nanosecond
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	rows := make([]tRow, 10)
	for i := range rows {
		ts := base.Add(time.Duration(i) * time.Hour)
		rows[i] = tRow{Millis: ts, Micros: ts, Nanos: ts}
	}

	data := writeGeneric(t, rows)
	lo := base.Add(3 * time.Hour)
	hi := base.Add(6 * time.Hour)

	cases := []struct {
		name string
		pred parquetfast.Predicate
		want int
	}{
		{"millis", parquetfast.Col("millis").Between(lo, hi), 4}, // hours 3,4,5,6
		{"micros", parquetfast.Col("micros").Greater(hi), 3},     // hours 7,8,9
		{"nanos", parquetfast.Col("nanos").LessOrEqual(lo), 4},   // hours 0,1,2,3
	}

	for _, tc := range cases {
		got, err := parquetfast.UnmarshalBytes[tRow](data, parquetfast.Where(tc.pred))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}

		if len(got) != tc.want {
			t.Fatalf("%s: got %d rows, want %d", tc.name, len(got), tc.want)
		}
	}

	// DATE unit: an INT32 days-since-epoch column filtered with a time.Time.
	type dRow struct {
		D int32 `parquet:"d,date"`
	}

	dbase := int32(base.Unix() / 86400)

	drows := make([]dRow, 10)
	for i := range drows {
		drows[i] = dRow{D: dbase + int32(i)}
	}

	ddata := writeGeneric(t, drows)
	cutoff := base.AddDate(0, 0, 4)

	got, err := parquetfast.UnmarshalBytes[dRow](ddata, parquetfast.Where(parquetfast.Col("d").GreaterOrEqual(cutoff)))
	if err != nil {
		t.Fatalf("date filter: %v", err)
	}

	if len(got) != 6 { // days 4..9
		t.Fatalf("date filter: got %d rows, want 6", len(got))
	}
}

// ── map[K]time.Time value path (addTimeValuedMap) ─────────────────────────────

func TestCoverage_MapValuedTime(t *testing.T) {
	t.Parallel()

	type row struct {
		ID   int64                `parquet:"id"`
		Seen map[string]time.Time `parquet:"seen"`
	}

	base := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)

	in := []row{
		{ID: 1, Seen: map[string]time.Time{"login": base, "logout": base.Add(time.Hour)}},
		{ID: 2, Seen: map[string]time.Time{"login": base.Add(48 * time.Hour)}},
		{ID: 3, Seen: nil},
	}

	data := writeGeneric(t, in)

	got, err := parquetfast.UnmarshalBytes[row](data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != len(in) {
		t.Fatalf("got %d rows, want %d", len(got), len(in))
	}

	for i := range in {
		if got[i].ID != in[i].ID {
			t.Fatalf("row %d: id %d, want %d", i, got[i].ID, in[i].ID)
		}

		if len(got[i].Seen) != len(in[i].Seen) {
			t.Fatalf("row %d: map len %d, want %d", i, len(got[i].Seen), len(in[i].Seen))
		}

		for k, want := range in[i].Seen {
			gv, ok := got[i].Seen[k]
			if !ok {
				t.Fatalf("row %d: missing key %q", i, k)
			}

			if !gv.Equal(want) {
				t.Fatalf("row %d key %q: got %s, want %s", i, k, gv, want)
			}
		}
	}
}

// ── Not over every comparison operator (negateLeaf) ───────────────────────────

func TestCoverage_NotComparisons(t *testing.T) {
	t.Parallel()

	data := filterFixture(t, 300, 50)

	assertFilter(t, data, func(r filterRow) bool { return r.ID >= 100 },
		parquetfast.Not(parquetfast.Col("id").Less(int64(100))))
	assertFilter(t, data, func(r filterRow) bool { return r.ID > 100 },
		parquetfast.Not(parquetfast.Col("id").LessOrEqual(int64(100))))
	assertFilter(t, data, func(r filterRow) bool { return r.ID <= 200 },
		parquetfast.Not(parquetfast.Col("id").Greater(int64(200))))
	assertFilter(t, data, func(r filterRow) bool { return r.ID < 200 },
		parquetfast.Not(parquetfast.Col("id").GreaterOrEqual(int64(200))))
}
