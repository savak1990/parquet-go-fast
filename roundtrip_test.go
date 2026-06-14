package parquetfast_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// roundtrip writes rows with parquet-go's GenericWriter, decodes them back with
// parquet-go-fast, and asserts equality. This is the primary correctness gate:
// encode with a conformant writer, decode with this library.
func roundtrip[T any](t *testing.T, rows []T, writerOpts ...parquet.WriterOption) {
	t.Helper()

	buf := writeGeneric(t, rows, writerOpts...)

	got, err := parquetfast.UnmarshalBytes[T](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	if !reflect.DeepEqual(rows, got) {
		t.Fatalf("roundtrip mismatch:\n want %#v\n got  %#v", rows, got)
	}
}

func writeGeneric[T any](t *testing.T, rows []T, writerOpts ...parquet.WriterOption) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[T](&buf, writerOpts...)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	return buf.Bytes()
}

// ── Scalars (incl. narrow ints and []byte) ───────────────────────────────────

type scalarRow struct {
	S   string  `parquet:"s"`
	B   bool    `parquet:"b"`
	I   int     `parquet:"i"`
	I8  int8    `parquet:"i8"`
	I16 int16   `parquet:"i16"`
	I32 int32   `parquet:"i32"`
	I64 int64   `parquet:"i64"`
	U16 uint16  `parquet:"u16"`
	U32 uint32  `parquet:"u32"`
	U64 uint64  `parquet:"u64"`
	F32 float32 `parquet:"f32"`
	F64 float64 `parquet:"f64"`
	Bs  []byte  `parquet:"bs"`
}

func TestScalars(t *testing.T) {
	t.Parallel()

	rows := make([]scalarRow, 4)
	for i := range rows {
		rows[i] = scalarRow{
			S:   "row-" + string(rune('a'+i)),
			B:   i%2 == 0,
			I:   i * 1000,
			I8:  int8(i - 2),
			I16: int16(i * 7),
			I32: int32(i * 100000),
			I64: int64(i) * 1 << 40,
			U16: uint16(i * 9),
			U32: uint32(i * 200000),
			U64: uint64(i) * 1 << 50,
			F32: float32(i) * 1.5,
			F64: float64(i) * 3.25,
			Bs:  []byte{byte(i), byte(i + 1), byte(i + 2)},
		}
	}

	roundtrip(t, rows)
}

// ── Optionals (*T incl. *[]byte), present and nil ────────────────────────────

type optionalRow struct {
	S   *string  `parquet:"s,optional"`
	B   *bool    `parquet:"b,optional"`
	I   *int     `parquet:"i,optional"`
	I8  *int8    `parquet:"i8,optional"`
	I32 *int32   `parquet:"i32,optional"`
	I64 *int64   `parquet:"i64,optional"`
	U32 *uint32  `parquet:"u32,optional"`
	F64 *float64 `parquet:"f64,optional"`
	Bs  *[]byte  `parquet:"bs,optional"`
}

func TestOptionals(t *testing.T) {
	t.Parallel()

	s := "hello"
	b := true
	i := 7
	i8 := int8(-3)
	i32 := int32(123456)
	i64 := int64(9 << 40)
	u32 := uint32(42)
	f := 2.71828
	bs := []byte{1, 2, 3}

	rows := []optionalRow{
		{S: &s, B: &b, I: &i, I8: &i8, I32: &i32, I64: &i64, U32: &u32, F64: &f, Bs: &bs},
		{}, // all nil
		{S: &s, I64: &i64, Bs: &bs},
	}

	roundtrip(t, rows)
}

// ── Primitive slices ─────────────────────────────────────────────────────────

type primitiveSliceRow struct {
	Strs []string  `parquet:"strs"`
	I32s []int32   `parquet:"i32s"`
	I64s []int64   `parquet:"i64s"`
	U32s []uint32  `parquet:"u32s"`
	F64s []float64 `parquet:"f64s"`
	Bls  []bool    `parquet:"bls"`
}

func TestPrimitiveSlices(t *testing.T) {
	t.Parallel()

	rows := []primitiveSliceRow{
		{
			Strs: []string{"a", "b", "c"},
			I32s: []int32{1, 2, 3},
			I64s: []int64{10, 20},
			U32s: []uint32{5, 6, 7, 8},
			F64s: []float64{1.1, 2.2},
			Bls:  []bool{true, false, true},
		},
		{
			Strs: []string{"x"},
			I32s: []int32{99},
			I64s: []int64{1 << 40},
			U32s: []uint32{0},
			F64s: []float64{3.3},
			Bls:  []bool{false},
		},
	}

	roundtrip(t, rows)
}

// ── Required nested struct ───────────────────────────────────────────────────

type inner struct {
	A string `parquet:"a"`
	B int64  `parquet:"b"`
}

type requiredStructRow struct {
	Name  string `parquet:"name"`
	Inner inner  `parquet:"inner"`
}

func TestRequiredStruct(t *testing.T) {
	t.Parallel()

	rows := []requiredStructRow{
		{Name: "one", Inner: inner{A: "x", B: 1}},
		{Name: "two", Inner: inner{A: "y", B: 2}},
	}

	roundtrip(t, rows)
}

// ── Schema evolution: write subset, decode into superset ─────────────────────

type narrowWrite struct {
	A string `parquet:"a"`
}

type wideRead struct {
	A string `parquet:"a"`
	B int64  `parquet:"b"` // absent in the file
}

func TestSchemaEvolution(t *testing.T) {
	t.Parallel()

	buf := writeGeneric(t, []narrowWrite{{A: "x"}, {A: "y"}})

	got, err := parquetfast.UnmarshalBytes[wideRead](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	want := []wideRead{{A: "x", B: 0}, {A: "y", B: 0}}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("schema-evolution mismatch:\n want %#v\n got  %#v", want, got)
	}
}

// ── Empty file ───────────────────────────────────────────────────────────────

func TestEmpty(t *testing.T) {
	t.Parallel()

	buf := writeGeneric(t, []scalarRow{})

	got, err := parquetfast.UnmarshalBytes[scalarRow](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(got))
	}
}

func TestUnmarshalFile(t *testing.T) {
	t.Parallel()

	rows := []scalarRow{
		{S: "a", I64: 1, Bs: []byte{1}},
		{S: "b", I64: 2, Bs: []byte{2}},
	}
	buf := writeGeneric(t, rows)

	path := filepath.Join(t.TempDir(), "f.parquet")
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := parquetfast.UnmarshalFile[scalarRow](path)
	if err != nil {
		t.Fatalf("UnmarshalFile: %v", err)
	}

	if !reflect.DeepEqual(rows, got) {
		t.Fatalf("mismatch:\n want %#v\n got  %#v", rows, got)
	}
}
