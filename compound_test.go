package parquetfast_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// ── Optional struct (present / absent), registered + fallback ─────────────────

type optStructInner struct {
	A string `parquet:"a"`
	B int64  `parquet:"b"`
}

type optStructRow struct {
	Name  string          `parquet:"name"`
	Inner *optStructInner `parquet:"inner,optional"`
}

func init() {
	// Typed fast path for the optional-struct test; the reflect fallback is
	// covered by optStructRow2 (deliberately unregistered).
	parquetfast.RegisterStructAlloc[optStructInner]()
}

func TestOptionalStruct(t *testing.T) {
	rows := []optStructRow{
		{Name: "has", Inner: &optStructInner{A: "x", B: 1}},
		{Name: "nil", Inner: nil},
		{Name: "has2", Inner: &optStructInner{A: "y", B: 2}},
	}

	roundtrip(t, rows)
}

type optStructInner2 struct {
	C string `parquet:"c"`
}

type optStructRow2 struct {
	Name  string           `parquet:"name"`
	Inner *optStructInner2 `parquet:"inner,optional"` // not registered → reflect.New fallback
}

func TestOptionalStructReflectFallback(t *testing.T) {
	rows := []optStructRow2{
		{Name: "has", Inner: &optStructInner2{C: "z"}},
		{Name: "nil"},
	}

	roundtrip(t, rows)
}

// ── Struct list, registered + reflect fallback ───────────────────────────────

type listItem struct {
	A string `parquet:"a"`
	B int64  `parquet:"b"`
}

type structListRow struct {
	Name  string     `parquet:"name"`
	Items []listItem `parquet:"items"`
}

func init() {
	parquetfast.RegisterStructList[listItem]()
}

func TestStructListRegistered(t *testing.T) {
	rows := []structListRow{
		{Name: "one", Items: []listItem{{A: "x", B: 1}, {A: "y", B: 2}}},
		{Name: "two", Items: []listItem{{A: "z", B: 3}}},
	}

	roundtrip(t, rows)
}

type listItem2 struct {
	C string `parquet:"c"`
}

type structListRow2 struct {
	Name  string      `parquet:"name"`
	Items []listItem2 `parquet:"items"` // unregistered → reflect.MakeSlice fallback
}

func TestStructListReflectFallback(t *testing.T) {
	rows := []structListRow2{
		{Name: "one", Items: []listItem2{{C: "a"}, {C: "b"}}},
		{Name: "two", Items: []listItem2{{C: "c"}}},
	}

	roundtrip(t, rows)
}

// ── Primitive maps (typed fast paths + reflect fallback) ─────────────────────

type primitiveMapRow struct {
	SS   map[string]string `parquet:"ss"`
	SI   map[string]int64  `parquet:"si"`
	IF   map[int64]float64 `parquet:"if"`
	I32S map[int32]string  `parquet:"i32s"` // reflect fallback combo
}

func TestPrimitiveMaps(t *testing.T) {
	rows := []primitiveMapRow{
		{
			SS:   map[string]string{"a": "1", "b": "2"},
			SI:   map[string]int64{"x": 10, "y": 20},
			IF:   map[int64]float64{1: 1.5, 2: 2.5},
			I32S: map[int32]string{7: "seven"},
		},
		{
			SS:   map[string]string{"c": "3"},
			SI:   map[string]int64{"z": 30},
			IF:   map[int64]float64{3: 3.5},
			I32S: map[int32]string{8: "eight", 9: "nine"},
		},
	}

	roundtrip(t, rows)
}

// ── Struct-valued map, registered + reflect fallback ─────────────────────────

type containerStats struct {
	Name string `parquet:"name"`
	CPU  int64  `parquet:"cpu"`
}

type structMapRow struct {
	Name       string                    `parquet:"name"`
	Containers map[string]containerStats `parquet:"containers"`
}

func init() {
	parquetfast.RegisterStructValuedMap[string, containerStats](func(v parquet.Value) string {
		return string(v.ByteArray())
	})
}

func TestStructValuedMapRegistered(t *testing.T) {
	rows := []structMapRow{
		{Name: "p1", Containers: map[string]containerStats{
			"app":     {Name: "app", CPU: 100},
			"sidecar": {Name: "sidecar", CPU: 50},
		}},
		{Name: "p2", Containers: map[string]containerStats{
			"app": {Name: "app", CPU: 200},
		}},
	}

	roundtrip(t, rows)
}

type containerStats2 struct {
	V int64 `parquet:"v"`
}

type structMapRow2 struct {
	Name string                     `parquet:"name"`
	M    map[string]containerStats2 `parquet:"m"` // unregistered → reflect fallback
}

func TestStructValuedMapReflectFallback(t *testing.T) {
	rows := []structMapRow2{
		{Name: "a", M: map[string]containerStats2{"k1": {V: 1}, "k2": {V: 2}}},
		{Name: "b", M: map[string]containerStats2{"k3": {V: 3}}},
	}

	roundtrip(t, rows)
}

// ── Nested map ───────────────────────────────────────────────────────────────

type nestedMapRow struct {
	Name    string                        `parquet:"name"`
	Targets map[string]map[string]float64 `parquet:"targets"`
}

func TestNestedMap(t *testing.T) {
	rows := []nestedMapRow{
		{Name: "a", Targets: map[string]map[string]float64{
			"cpu":    {"util": 0.8, "limit": 1.0},
			"memory": {"util": 0.5},
		}},
		{Name: "b", Targets: map[string]map[string]float64{
			"cpu": {"util": 0.3},
		}},
	}

	roundtrip(t, rows)
}

// ── Multi-row-group (force ≥2 RGs) + an all-null column ───────────────────────

type mrgRow struct {
	ID    int64   `parquet:"id"`
	Name  string  `parquet:"name"`
	Maybe *string `parquet:"maybe,optional"` // left nil in every row → all-null column
}

func TestMultiRowGroup(t *testing.T) {
	const n = 50

	rows := make([]mrgRow, n)
	for i := range rows {
		rows[i] = mrgRow{ID: int64(i), Name: "row-" + string(rune('A'+i%26))}
	}

	// MaxRowsPerRowGroup forces several row groups.
	buf := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(10))

	f, _ := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	if len(f.RowGroups()) < 2 {
		t.Fatalf("expected ≥2 row groups, got %d", len(f.RowGroups()))
	}

	got, err := parquetfast.UnmarshalBytes[mrgRow](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	if !reflect.DeepEqual(rows, got) {
		t.Fatalf("multi-row-group mismatch:\n want %#v\n got  %#v", rows, got)
	}
}

// ── Reader[T] streaming ──────────────────────────────────────────────────────

func TestReaderStreaming(t *testing.T) {
	const n = 37

	rows := make([]scalarRow, n)
	for i := range rows {
		rows[i] = scalarRow{S: "r", I64: int64(i), Bs: []byte{byte(i)}}
	}

	buf := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(8))

	rd, err := parquetfast.NewReader[scalarRow](bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()

	if rd.NumRows() != n {
		t.Fatalf("NumRows = %d, want %d", rd.NumRows(), n)
	}

	var got []scalarRow

	dst := make([]scalarRow, 10)
	for {
		m, err := rd.Read(dst)
		got = append(got, dst[:m]...)

		if err != nil {
			break
		}
	}

	if !reflect.DeepEqual(rows, got) {
		t.Fatalf("reader mismatch: got %d rows, want %d", len(got), n)
	}
}
