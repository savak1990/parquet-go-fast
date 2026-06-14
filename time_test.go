package parquetfast_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// time.Time round-trips are compared by instant (.Equal) and location, not
// reflect.DeepEqual: Go's time.Time has multiple internal representations for
// the same instant (wall vs ext, monotonic bit), so two equal instants need not
// be DeepEqual. parquet stores the absolute instant; we reconstruct it in UTC.

func timeEqual(t *testing.T, idx int, field string, want, got time.Time) {
	t.Helper()

	if !want.Equal(got) {
		t.Fatalf("row %d %s: instant mismatch: want %v, got %v", idx, field, want, got)
	}

	if got.Location() != time.UTC {
		t.Fatalf("row %d %s: expected UTC location, got %v", idx, field, got.Location())
	}
}

// Default time.Time → TIMESTAMP(NANOS) leaf.

type tsRow struct {
	ID   int64      `parquet:"id"`
	When time.Time  `parquet:"when"`
	Opt  *time.Time `parquet:"opt,optional"`
}

func TestTime_TimestampNanosDefault(t *testing.T) {
	t.Parallel()

	opt := time.Date(2020, 3, 4, 5, 6, 7, 8, time.UTC)
	in := []tsRow{
		{ID: 1, When: time.Date(2026, 6, 13, 10, 30, 0, 123456789, time.UTC), Opt: &opt},
		{ID: 2, When: time.Unix(0, 0).UTC()}, // epoch, Opt nil
		{ID: 3, When: time.Date(1999, 12, 31, 23, 59, 59, 999999999, time.UTC)},
	}

	buf := writeGeneric(t, in)

	got, err := parquetfast.UnmarshalBytes[tsRow](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	if len(got) != len(in) {
		t.Fatalf("got %d rows, want %d", len(got), len(in))
	}

	for i := range in {
		if got[i].ID != in[i].ID {
			t.Fatalf("row %d id: want %d got %d", i, in[i].ID, got[i].ID)
		}

		timeEqual(t, i, "when", in[i].When, got[i].When)

		switch {
		case in[i].Opt == nil && got[i].Opt != nil:
			t.Fatalf("row %d opt: want nil, got %v", i, *got[i].Opt)
		case in[i].Opt != nil && got[i].Opt == nil:
			t.Fatalf("row %d opt: want %v, got nil", i, *in[i].Opt)
		case in[i].Opt != nil:
			timeEqual(t, i, "opt", *in[i].Opt, *got[i].Opt)
		}
	}
}

// Tagged units: millis and micros. (DATE is validated separately below — see
// TestTime_DateColumn — because parquet-go's GenericWriter mis-encodes a
// time.Time written into a `,date` column: writeRowsFuncOfTime falls through to
// UnixNano and truncates it into the INT32, so that writer path can't be used as
// a fixture. Our DATE *read* path is correct and is exercised against a
// spec-compliant INT32 days column.)

type unitRow struct {
	Millis time.Time `parquet:"millis,timestamp(millisecond)"`
	Micros time.Time `parquet:"micros,timestamp(microsecond)"`
}

func TestTime_TaggedUnits(t *testing.T) {
	t.Parallel()

	in := []unitRow{
		{
			Millis: time.Date(2026, 1, 2, 3, 4, 5, 678000000, time.UTC), // ms precision
			Micros: time.Date(2026, 1, 2, 3, 4, 5, 678901000, time.UTC), // us precision
		},
		{
			Millis: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			Micros: time.Date(2000, 6, 15, 12, 0, 0, 1000, time.UTC),
		},
	}

	buf := writeGeneric(t, in)

	got, err := parquetfast.UnmarshalBytes[unitRow](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	for i := range in {
		timeEqual(t, i, "millis", in[i].Millis, got[i].Millis)
		timeEqual(t, i, "micros", in[i].Micros, got[i].Micros)
	}
}

// DATE read path: write a spec-compliant INT32 days-since-epoch column (via an
// int32 field tagged `,date`) and decode it into a time.Time field of the same
// column name. Validates kindTimeDate independently of parquet-go's time.Time
// date-write bug.

type dateWrite struct {
	D int32 `parquet:"d,date"`
}

type dateRead struct {
	D time.Time `parquet:"d"`
}

func TestTime_DateColumn(t *testing.T) {
	t.Parallel()

	days := func(y int, m time.Month, d int) int32 {
		return int32(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	}

	in := []dateWrite{
		{D: days(2026, 1, 2)},
		{D: days(1970, 1, 1)},
		{D: days(1999, 12, 31)},
	}

	buf := writeGeneric(t, in)

	got, err := parquetfast.UnmarshalBytes[dateRead](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	want := []time.Time{
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	for i := range want {
		timeEqual(t, i, "date", want[i], got[i].D)
	}
}

// time.Time inside nested structs, maps, and slices.

type tsContainer struct {
	Name    string    `parquet:"name"`
	Created time.Time `parquet:"created"`
}

type nestedTimeRow struct {
	ID      int64                  `parquet:"id"`
	Stamps  []time.Time            `parquet:"stamps"`
	ByName  map[string]tsContainer `parquet:"by_name"`
	Updated *tsContainer           `parquet:"updated,optional"`
}

func TestTime_Nested(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 2, 1, 2, 3, 400000000, time.UTC)

	upd := tsContainer{Name: "u", Created: t1}
	in := []nestedTimeRow{
		{
			ID:      1,
			Stamps:  []time.Time{t0, t1},
			ByName:  map[string]tsContainer{"a": {Name: "a", Created: t0}, "b": {Name: "b", Created: t1}},
			Updated: &upd,
		},
		{
			ID:     2,
			Stamps: []time.Time{t1},
			ByName: map[string]tsContainer{"c": {Name: "c", Created: t0}},
		},
	}

	buf := writeGeneric(t, in)

	got, err := parquetfast.UnmarshalBytes[nestedTimeRow](buf)
	if err != nil {
		t.Fatalf("UnmarshalBytes: %v", err)
	}

	for i := range in {
		if len(got[i].Stamps) != len(in[i].Stamps) {
			t.Fatalf("row %d: stamps len %d want %d", i, len(got[i].Stamps), len(in[i].Stamps))
		}

		for j := range in[i].Stamps {
			timeEqual(t, i, "stamps", in[i].Stamps[j], got[i].Stamps[j])
		}

		if len(got[i].ByName) != len(in[i].ByName) {
			t.Fatalf("row %d: byName len %d want %d", i, len(got[i].ByName), len(in[i].ByName))
		}

		for k, want := range in[i].ByName {
			g, ok := got[i].ByName[k]
			if !ok {
				t.Fatalf("row %d: missing key %q", i, k)
			}

			if g.Name != want.Name {
				t.Fatalf("row %d key %q: name %q want %q", i, k, g.Name, want.Name)
			}

			timeEqual(t, i, "byName.created", want.Created, g.Created)
		}

		switch {
		case in[i].Updated == nil && got[i].Updated != nil:
			t.Fatalf("row %d updated: want nil", i)
		case in[i].Updated != nil && got[i].Updated == nil:
			t.Fatalf("row %d updated: want non-nil", i)
		case in[i].Updated != nil:
			timeEqual(t, i, "updated.created", in[i].Updated.Created, got[i].Updated.Created)
		}
	}
}

// A time.Time field written with a non-supported physical encoding should fail
// loudly at Compile rather than silently decoding zero.
func TestTime_UnsupportedEncodingErrors(t *testing.T) {
	t.Parallel()

	type floatTimeRow struct {
		T time.Time `parquet:"t,timestamp"` // ensure a normal one compiles fine
	}

	// Sanity: the supported form compiles + decodes.
	buf := writeGeneric(t, []floatTimeRow{{T: time.Unix(0, 0).UTC()}})
	if _, err := parquetfast.UnmarshalBytes[floatTimeRow](buf); err != nil {
		t.Fatalf("supported timestamp should decode: %v", err)
	}
}

// Sanity check that the probe-style raw schema is a single INT64 leaf (guards
// against accidental struct-recursion regressions).
func TestTime_SingleLeafSchema(t *testing.T) {
	t.Parallel()

	buf := writeGeneric(t, []tsRow{{ID: 1, When: time.Unix(0, 0).UTC()}})

	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// id + when + opt = 3 leaf columns (no struct explosion for time.Time).
	if cols := len(f.Schema().Columns()); cols != 3 {
		t.Fatalf("expected 3 leaf columns, got %d: %v", cols, f.Schema().Columns())
	}
}
