package parquetfast_test

import (
	"bytes"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

// Column projection: decoding into a struct with a subset of the file's fields
// reads only those columns — the rest are skipped in the read pipeline. These
// tests verify (a) the result is correct and matches the corresponding fields of
// a full decode, and (b) projection genuinely reads fewer bytes.

type projItem struct {
	SKU string `parquet:"sku"`
	Qty int64  `parquet:"qty"`
}

type projStats struct {
	Min int64 `parquet:"min"`
	Max int64 `parquet:"max"`
}

// projFull is the wide on-disk record.
type projFull struct {
	A      int64             `parquet:"a"`
	B      int64             `parquet:"b"`
	C      float64           `parquet:"c"`
	D      string            `parquet:"d"`
	E      string            `parquet:"e"`
	Blob   []byte            `parquet:"blob"`
	Labels map[string]string `parquet:"labels"`
	Items  []projItem        `parquet:"items"`
	Stats  *projStats        `parquet:"stats,optional"`
	When   time.Time         `parquet:"when"`
}

func init() {
	parquetfast.RegisterStructList[projItem]()
	parquetfast.RegisterStructAlloc[projStats]()
}

func makeProjFull(i int) projFull {
	r := projFull{
		A: int64(i), B: int64(i * 2), C: float64(i) * 1.5,
		D: "d" + string(rune('a'+i%26)), E: "e" + string(rune('a'+i%26)),
		Blob:   blob(i, 32),
		Labels: map[string]string{"k": "v", "n": "m"},
		Items:  []projItem{{SKU: "s1", Qty: int64(i)}, {SKU: "s2", Qty: int64(i + 1)}},
		When:   time.Unix(1_700_000_000+int64(i), 0).UTC(),
	}
	if i%2 == 0 {
		r.Stats = &projStats{Min: int64(i), Max: int64(i + 100)}
	}

	return r
}

func projFixture(t *testing.T, n int) []byte {
	t.Helper()

	rows := make([]projFull, n)
	for i := range rows {
		rows[i] = makeProjFull(i)
	}

	return writeGeneric(t, rows)
}

// ── Correctness: each narrow subset matches the full decode ───────────────────

func TestProjection_Scalars(t *testing.T) {
	type narrow struct {
		A int64  `parquet:"a"`
		E string `parquet:"e"`
	}

	buf := projFixture(t, 50)

	got, err := parquetfast.UnmarshalBytes[narrow](buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for i := range got {
		want := narrow{A: int64(i), E: "e" + string(rune('a'+i%26))}
		if got[i] != want {
			t.Fatalf("row %d: got %+v want %+v", i, got[i], want)
		}
	}
}

func TestProjection_OmitsMapSliceStructTime(t *testing.T) {
	// A narrow struct that keeps a scalar + the map, dropping slice/struct/time.
	type narrow struct {
		A      int64             `parquet:"a"`
		Labels map[string]string `parquet:"labels"`
	}

	buf := projFixture(t, 30)

	got, err := parquetfast.UnmarshalBytes[narrow](buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for i := range got {
		if got[i].A != int64(i) {
			t.Fatalf("row %d A: got %d", i, got[i].A)
		}

		if !reflect.DeepEqual(got[i].Labels, map[string]string{"k": "v", "n": "m"}) {
			t.Fatalf("row %d labels: got %+v", i, got[i].Labels)
		}
	}
}

func TestProjection_KeepSliceDropRest(t *testing.T) {
	type narrow struct {
		A     int64      `parquet:"a"`
		Items []projItem `parquet:"items"`
	}

	buf := projFixture(t, 20)

	got, err := parquetfast.UnmarshalBytes[narrow](buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for i := range got {
		want := []projItem{{SKU: "s1", Qty: int64(i)}, {SKU: "s2", Qty: int64(i + 1)}}
		if !reflect.DeepEqual(got[i].Items, want) {
			t.Fatalf("row %d items: got %+v want %+v", i, got[i].Items, want)
		}
	}
}

func TestProjection_KeepOptionalStructDropRest(t *testing.T) {
	type narrow struct {
		A     int64      `parquet:"a"`
		Stats *projStats `parquet:"stats,optional"`
	}

	buf := projFixture(t, 20)

	got, err := parquetfast.UnmarshalBytes[narrow](buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for i := range got {
		if got[i].A != int64(i) {
			t.Fatalf("row %d A", i)
		}

		if i%2 == 0 {
			if got[i].Stats == nil || *got[i].Stats != (projStats{Min: int64(i), Max: int64(i + 100)}) {
				t.Fatalf("row %d stats: got %+v", i, got[i].Stats)
			}
		} else if got[i].Stats != nil {
			t.Fatalf("row %d stats: expected nil, got %+v", i, *got[i].Stats)
		}
	}
}

func TestProjection_KeepTimeDropRest(t *testing.T) {
	type narrow struct {
		A    int64     `parquet:"a"`
		When time.Time `parquet:"when"`
	}

	buf := projFixture(t, 20)

	got, err := parquetfast.UnmarshalBytes[narrow](buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for i := range got {
		want := time.Unix(1_700_000_000+int64(i), 0).UTC()
		if !got[i].When.Equal(want) {
			t.Fatalf("row %d when: got %v want %v", i, got[i].When, want)
		}
	}
}

// Projection must not change results vs reading the full struct.
func TestProjection_MatchesFullDecodeSubset(t *testing.T) {
	type narrow struct {
		A int64  `parquet:"a"`
		D string `parquet:"d"`
	}

	buf := projFixture(t, 40)

	full, err := parquetfast.UnmarshalBytes[projFull](buf)
	if err != nil {
		t.Fatalf("full: %v", err)
	}

	narrowRows, err := parquetfast.UnmarshalBytes[narrow](buf)
	if err != nil {
		t.Fatalf("narrow: %v", err)
	}

	for i := range full {
		if narrowRows[i].A != full[i].A || narrowRows[i].D != full[i].D {
			t.Fatalf("row %d: narrow %+v vs full a=%d d=%q", i, narrowRows[i], full[i].A, full[i].D)
		}
	}
}

// ── Proof that projection reads fewer bytes ───────────────────────────────────

type countingReaderAt struct {
	r     *bytes.Reader
	bytes atomic.Int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.r.ReadAt(p, off)
	c.bytes.Add(int64(n))

	return n, err
}

func TestProjection_ReadsFewerBytes(t *testing.T) {
	type narrow struct {
		A int64  `parquet:"a"`
		E string `parquet:"e"`
	}

	buf := projFixture(t, 5000)
	size := int64(len(buf))

	read := func(opts ...parquetfast.Option) int64 {
		cr := &countingReaderAt{r: bytes.NewReader(buf)}
		if _, err := parquetfast.Unmarshal[narrow](cr, size, opts...); err != nil {
			t.Fatalf("decode: %v", err)
		}

		return cr.bytes.Load()
	}

	withProj := read()                                    // default: projection on
	noProj := read(parquetfast.WithoutColumnProjection()) // reads all columns

	t.Logf("bytes read: projection=%d, no-projection=%d (%.1f%% of full)",
		withProj, noProj, 100*float64(withProj)/float64(noProj))

	if withProj >= noProj {
		t.Fatalf("expected projection to read fewer bytes: %d >= %d", withProj, noProj)
	}
}

// ── Benchmark: full struct vs projected narrow struct on a wide file ──────────

func BenchmarkProjection(b *testing.B) {
	rows := make([]projFull, 100_000)
	for i := range rows {
		rows[i] = makeProjFull(i)
	}

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[projFull](&buf,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(50_000),
	)
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()

	b.Run("full-struct", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[projFull](data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("projected-2-scalars", func(b *testing.B) {
		type narrow struct {
			A int64  `parquet:"a"`
			E string `parquet:"e"`
		}

		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[narrow](data); err != nil {
				b.Fatal(err)
			}
		}
	})
}
