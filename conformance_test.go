package parquetfast_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Conformance tests against Apache's spec test corpus (apache/parquet-testing).
// These files are written by many producers (parquet-mr/Java, parquet-cpp,
// parquet-rs, Impala, Spark, Presto) using encodings and edge cases our own
// synthetic fixtures (written by parquet-go) never exercise: DELTA_*,
// BYTE_STREAM_SPLIT, RLE_DICTIONARY, Float16, INT96, decimals, LZ4/brotli,
// legacy 2-level lists, maps without required keys, null pages, etc.
//
// The corpus is not vendored. Point PARQUET_TESTING_DIR at a checkout's data/
// directory (git clone https://github.com/apache/parquet-testing); tests skip
// when it is absent.

func goldenDir(t *testing.T) string {
	t.Helper()

	dir := os.Getenv("PARQUET_TESTING_DIR")
	if dir == "" {
		t.Skip("set PARQUET_TESTING_DIR to a checkout of apache/parquet-testing's data/ dir")
	}

	return dir
}

func readGolden(t *testing.T, dir, file string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, file))
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}

	return data
}

// TestConformance_AllGoldenFilesDecode is the broad robustness sweep: for every
// golden file the reference reader can open, our library must iterate every row
// group and report the same row count, without error or panic. It decodes into
// an empty struct (zero columns) so it exercises the file/footer/row-group path
// across the whole encoding/compression/logical-type matrix.
func TestConformance_AllGoldenFilesDecode(t *testing.T) {
	t.Parallel()

	dir := goldenDir(t)

	files, _ := filepath.Glob(filepath.Join(dir, "*.parquet"))
	sort.Strings(files)

	if len(files) == 0 {
		t.Fatalf("no .parquet files under %s", dir)
	}

	var refReadable, ours int

	for _, f := range files {
		name := filepath.Base(f)
		data := readGolden(t, dir, name)

		// Reference gate: if parquet-go can't open the file, it's beyond the
		// shared substrate (e.g. metadata it can't parse) — not our concern.
		pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			continue
		}

		refReadable++
		want := pf.NumRows()

		got, derr := decodeRowCount(data)
		if derr != nil {
			t.Errorf("%s: our decode failed: %v", name, derr)

			continue
		}

		if int64(got) != want {
			t.Errorf("%s: row count = %d, reference = %d", name, got, want)

			continue
		}

		ours++
	}

	t.Logf("decoded %d/%d reference-readable golden files with matching row counts", ours, refReadable)
}

// decodeRowCount decodes into an empty struct and returns the row count,
// converting a panic into an error so one bad file can't crash the sweep.
func decodeRowCount(data []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicAsError(r)
		}
	}()

	rows, err := parquetfast.UnmarshalBytes[struct{}](data)
	if err != nil {
		return 0, err
	}

	return len(rows), nil
}

// ── Deep value check: our typed decode == the reference reader ─────────────────

// Curated golden files with hand-written Go types. For each, our UnmarshalBytes
// must produce exactly what parquet-go's (reflection-based, spec-conformant)
// GenericReader produces for the same type — proving our reflection-free fast
// path is faithful across these producer/encoding combinations.

type ctAllTypes struct {
	ID       int32   `parquet:"id"`
	Bool     bool    `parquet:"bool_col"`
	TinyInt  int32   `parquet:"tinyint_col"`
	SmallInt int32   `parquet:"smallint_col"`
	Int      int32   `parquet:"int_col"`
	BigInt   int64   `parquet:"bigint_col"`
	Float    float32 `parquet:"float_col"`
	Double   float64 `parquet:"double_col"`
	DateStr  []byte  `parquet:"date_string_col"`
	String   []byte  `parquet:"string_col"`
	// timestamp_col is INT96 (deprecated) — covered separately; omitted here so
	// both readers project the same 10 columns.
}

type ctBinary struct {
	Foo []byte `parquet:"foo"`
}

type ctRLEBool struct {
	B *bool `parquet:"datatype_boolean"`
}

type ctByteStreamSplit struct {
	F32 float32 `parquet:"f32"`
	F64 float64 `parquet:"f64"`
}

type ctDeltaByteArray struct {
	CustomerID   string `parquet:"c_customer_id"`
	Salutation   string `parquet:"c_salutation"`
	FirstName    string `parquet:"c_first_name"`
	LastName     string `parquet:"c_last_name"`
	PreferredCF  string `parquet:"c_preferred_cust_flag"`
	BirthCountry string `parquet:"c_birth_country"`
	Login        string `parquet:"c_login"`
	Email        string `parquet:"c_email_address"`
	LastReview   string `parquet:"c_last_review_date"`
}

type ctLists struct {
	Int64List []*int64  `parquet:"int64_list"`
	Utf8List  []*string `parquet:"utf8_list"`
}

type ctNestedMaps struct {
	A map[string]map[int32]bool `parquet:"a"`
	B int32                     `parquet:"b"`
	C float64                   `parquet:"c"`
}

type ctDecimalI64 struct {
	Value int64 `parquet:"value"`
}

func TestConformance_TypedMatchesReference(t *testing.T) {
	t.Parallel()

	dir := goldenDir(t)

	t.Run("alltypes_plain", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctAllTypes](t, dir, "alltypes_plain.parquet")
	})
	t.Run("alltypes_dictionary", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctAllTypes](t, dir, "alltypes_dictionary.parquet")
	})
	t.Run("alltypes_plain.snappy", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctAllTypes](t, dir, "alltypes_plain.snappy.parquet")
	})
	t.Run("binary", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctBinary](t, dir, "binary.parquet")
	})
	t.Run("rle_boolean", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctRLEBool](t, dir, "rle_boolean_encoding.parquet")
	})
	t.Run("byte_stream_split", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctByteStreamSplit](t, dir, "byte_stream_split.zstd.parquet")
	})
	t.Run("delta_byte_array", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctDeltaByteArray](t, dir, "delta_byte_array.parquet")
	})
	t.Run("nested_maps.snappy", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctNestedMaps](t, dir, "nested_maps.snappy.parquet")
	})
	t.Run("int64_decimal", func(t *testing.T) {
		t.Parallel()
		checkTyped[ctDecimalI64](t, dir, "int64_decimal.parquet")
	})
}

// TestConformance_NullableListElements decodes list_columns, which has NULL
// elements inside its lists ([1,2,3], [NULL,1], [4]) and a wholly-NULL list, so
// the spec-correct Go type is []*int64 / []*string.
//
// Two things make this file a good probe: (1) it needs nullable list elements
// (positions preserved, null → nil), and (2) its element node is named "item",
// not the spec-default "element". parquet-go's own GenericReader assumes
// "element" and silently returns empty lists; resolving the element structurally
// lets us decode it correctly — a case where we are strictly more correct than
// the reference reader.
func TestConformance_NullableListElements(t *testing.T) {
	t.Parallel()

	dir := goldenDir(t)
	data := readGolden(t, dir, "list_columns.parquet")

	// Document that the reference reader gets this wrong (returns empty lists).
	if ref, rerr := parquet.Read[ctLists](bytes.NewReader(data), int64(len(data))); rerr == nil && len(ref) == 3 {
		t.Logf("parquet-go GenericReader on this file: row0.int64_list len=%d (expected 3) — it assumes element name 'element'", len(ref[0].Int64List))
	}

	got, err := parquetfast.UnmarshalBytes[ctLists](data)
	if err != nil {
		t.Fatalf("decode list_columns: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}

	wantInt := [][]*int64{
		{ptr[int64](1), ptr[int64](2), ptr[int64](3)},
		{nil, ptr[int64](1)},
		{ptr[int64](4)},
	}
	wantStr := [][]*string{
		{ptr("abc"), ptr("efg"), ptr("hij")},
		nil, // row 1's utf8_list is a NULL list
		{ptr("efg"), nil, ptr("hij"), ptr("xyz")},
	}

	for i := range got {
		if !reflect.DeepEqual(got[i].Int64List, wantInt[i]) {
			t.Errorf("row %d int64_list mismatch:\n  got  %v\n  want %v", i, derefI64(got[i].Int64List), derefI64(wantInt[i]))
		}

		if !reflect.DeepEqual(got[i].Utf8List, wantStr[i]) {
			t.Errorf("row %d utf8_list mismatch:\n  got  %v\n  want %v", i, derefStr(got[i].Utf8List), derefStr(wantStr[i]))
		}
	}
}

func derefI64(s []*int64) []any {
	out := make([]any, len(s))
	for i, p := range s {
		if p == nil {
			out[i] = "nil"
		} else {
			out[i] = *p
		}
	}

	return out
}

func derefStr(s []*string) []any {
	out := make([]any, len(s))
	for i, p := range s {
		if p == nil {
			out[i] = "nil"
		} else {
			out[i] = *p
		}
	}

	return out
}

// checkTyped decodes file into []T with our library and with parquet-go's
// reference GenericReader, and asserts the results are identical. If the
// reference reader itself can't decode into T, the file is skipped (the mapping
// would be ambiguous for both).
func checkTyped[T any](t *testing.T, dir, file string) {
	t.Helper()

	data := readGolden(t, dir, file)

	ref, rerr := parquet.Read[T](bytes.NewReader(data), int64(len(data)))
	if rerr != nil {
		t.Skipf("reference reader cannot decode %s into %T: %v", file, *new(T), rerr)
	}

	ours, oerr := parquetfast.UnmarshalBytes[T](data)
	if oerr != nil {
		t.Fatalf("our decode of %s failed: %v", file, oerr)
	}

	if len(ours) != len(ref) {
		t.Fatalf("%s: row count ours=%d ref=%d", file, len(ours), len(ref))
	}

	mismatch := 0

	for i := range ref {
		if !reflect.DeepEqual(ours[i], ref[i]) {
			t.Errorf("%s row %d:\n  ours = %+v\n  ref  = %+v", file, i, ours[i], ref[i])

			if mismatch++; mismatch >= 3 {
				t.Fatalf("%s: too many mismatches, stopping", file)
			}
		}
	}
}

type panicError struct{ v any }

func (p panicError) Error() string {
	if e, ok := p.v.(error); ok {
		return "panic: " + e.Error()
	}

	if s, ok := p.v.(string); ok {
		return "panic: " + s
	}

	return "panic: non-string value"
}

func panicAsError(v any) error { return panicError{v} }
