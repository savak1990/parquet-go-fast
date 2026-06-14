package parquetfast_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	parquetfast "github.com/savak1990/parquet-go-fast"
)

type filterRow struct {
	ID     int64   `parquet:"id"`
	Region string  `parquet:"region"`
	Value  float64 `parquet:"value"`
}

func makeFilterRow(i int) filterRow {
	return filterRow{
		ID:     int64(i),
		Region: []string{"us", "eu", "apac"}[i%3],
		Value:  float64(i) * 1.5,
	}
}

func filterFixture(t *testing.T, n, rowsPerRG int) []byte {
	t.Helper()

	rows := make([]filterRow, n)
	for i := range rows {
		rows[i] = makeFilterRow(i)
	}

	return writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(int64(rowsPerRG)))
}

// assertFilter decodes with the predicates and checks the result equals a manual
// filter of the full decode (filtering preserves file order).
func assertFilter(t *testing.T, data []byte, want func(filterRow) bool, preds ...parquetfast.Predicate) {
	t.Helper()

	got, err := parquetfast.UnmarshalBytes[filterRow](data, parquetfast.Where(preds...))
	if err != nil {
		t.Fatalf("filtered decode: %v", err)
	}

	all, err := parquetfast.UnmarshalBytes[filterRow](data)
	if err != nil {
		t.Fatalf("full decode: %v", err)
	}

	var exp []filterRow
	for _, r := range all {
		if want(r) {
			exp = append(exp, r)
		}
	}

	if len(got) != len(exp) {
		t.Fatalf("count mismatch: got %d, want %d", len(got), len(exp))
	}

	for i := range exp {
		if got[i] != exp[i] {
			t.Fatalf("row %d: got %+v want %+v", i, got[i], exp[i])
		}
	}
}

func TestFilter_Operators(t *testing.T) {
	data := filterFixture(t, 300, 50) // 6 row groups, ids monotonic

	assertFilter(t, data, func(r filterRow) bool { return r.ID == 42 },
		parquetfast.Col("id").Equal(int64(42)))
	assertFilter(t, data, func(r filterRow) bool { return r.ID < 10 },
		parquetfast.Col("id").Less(int64(10)))
	assertFilter(t, data, func(r filterRow) bool { return r.ID <= 10 },
		parquetfast.Col("id").LessOrEqual(int64(10)))
	assertFilter(t, data, func(r filterRow) bool { return r.ID > 290 },
		parquetfast.Col("id").Greater(int64(290)))
	assertFilter(t, data, func(r filterRow) bool { return r.ID >= 290 },
		parquetfast.Col("id").GreaterOrEqual(int64(290)))
	assertFilter(t, data, func(r filterRow) bool { return r.ID >= 100 && r.ID <= 150 },
		parquetfast.Col("id").Between(int64(100), int64(150)))
	assertFilter(t, data, func(r filterRow) bool { return r.Region == "eu" },
		parquetfast.Col("region").Equal("eu"))
	// AND of two predicates.
	assertFilter(t, data, func(r filterRow) bool { return r.Region == "us" && r.ID >= 100 },
		parquetfast.Col("region").Equal("us"), parquetfast.Col("id").GreaterOrEqual(int64(100)))
	// Match nothing.
	assertFilter(t, data, func(r filterRow) bool { return false },
		parquetfast.Col("id").Greater(int64(10000)))
}

func TestFilter_OnColumnNotInStruct(t *testing.T) {
	data := filterFixture(t, 300, 50)

	// Destination omits Region; filter on it anyway.
	type idOnly struct {
		ID int64 `parquet:"id"`
	}

	got, err := parquetfast.UnmarshalBytes[idOnly](data, parquetfast.Where(parquetfast.Col("region").Equal("eu")))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// region cycles us,eu,apac → "eu" is every i%3==1 → ids 1,4,7,...
	for _, r := range got {
		if r.ID%3 != 1 {
			t.Fatalf("unexpected id %d (region should be eu)", r.ID)
		}
	}

	if len(got) != 100 {
		t.Fatalf("expected 100 eu rows, got %d", len(got))
	}
}

func TestFilter_PrunesRowGroups(t *testing.T) {
	data := filterFixture(t, 2000, 100) // 20 row groups, ids [0,100),[100,200),...

	// Confirm the writer recorded min/max stats (otherwise pruning can't fire).
	f, _ := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	cc := f.RowGroups()[0].ColumnChunks()[0].(*parquet.FileColumnChunk)
	if _, _, ok := cc.Bounds(); !ok {
		t.Skip("writer did not record column statistics; pruning can't be measured")
	}

	read := func(opts ...parquetfast.Option) int64 {
		cr := &countingReaderAt{r: bytes.NewReader(data)}
		if _, err := parquetfast.Unmarshal[filterRow](cr, int64(len(data)), opts...); err != nil {
			t.Fatalf("decode: %v", err)
		}

		return cr.bytes.Load()
	}

	full := read()
	// ids 150..160 live entirely in row group 1 ([100,200)); 19/20 groups pruned.
	filtered := read(parquetfast.Where(parquetfast.Col("id").Between(int64(150), int64(160))))

	t.Logf("bytes read: full=%d, filtered=%d (%.1f%% of full)",
		full, filtered, 100*float64(filtered)/float64(full))

	if filtered >= full {
		t.Fatalf("expected filtered read to be smaller: %d >= %d", filtered, full)
	}

	// Correctness: exactly ids 150..160.
	got, _ := parquetfast.UnmarshalBytes[filterRow](data, parquetfast.Where(parquetfast.Col("id").Between(int64(150), int64(160))))
	if len(got) != 11 {
		t.Fatalf("expected 11 rows (150..160), got %d", len(got))
	}

	for _, r := range got {
		if r.ID < 150 || r.ID > 160 {
			t.Fatalf("row out of range: %d", r.ID)
		}
	}
}

func TestFilter_TimeRange(t *testing.T) {
	type tfRow struct {
		ID   int64     `parquet:"id"`
		When time.Time `parquet:"when"`
	}

	base := time.Unix(1_700_000_000, 0).UTC()

	rows := make([]tfRow, 500)
	for i := range rows {
		rows[i] = tfRow{ID: int64(i), When: base.Add(time.Duration(i) * time.Minute)}
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(50))

	lo := base.Add(100 * time.Minute)
	hi := base.Add(110 * time.Minute)

	got, err := parquetfast.UnmarshalBytes[tfRow](data, parquetfast.Where(parquetfast.Col("when").Between(lo, hi)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 11 { // minutes 100..110 inclusive
		t.Fatalf("expected 11 rows, got %d", len(got))
	}

	for _, r := range got {
		if r.When.Before(lo) || r.When.After(hi) {
			t.Fatalf("row out of time range: %v", r.When)
		}
	}
}

func TestFilter_BloomPrunesEqualityMiss(t *testing.T) {
	rows := make([]filterRow, 1000)
	for i := range rows {
		rows[i] = makeFilterRow(i)
	}

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[filterRow](&buf,
		parquet.MaxRowsPerRowGroup(100),
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "region")),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data := buf.Bytes()

	// A region value that doesn't exist → bloom should prune every row group.
	got, err := parquetfast.UnmarshalBytes[filterRow](data, parquetfast.Where(parquetfast.Col("region").Equal("does-not-exist")))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 rows for absent region, got %d", len(got))
	}

	// An existing value still returns the right rows.
	got, _ = parquetfast.UnmarshalBytes[filterRow](data, parquetfast.Where(parquetfast.Col("region").Equal("eu")))
	for _, r := range got {
		if r.Region != "eu" {
			t.Fatalf("got non-eu row: %+v", r)
		}
	}
}

func BenchmarkFilter(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping filter benchmark in -short mode")
	}

	const n = 1_000_000

	rows := make([]filterRow, n)
	for i := range rows {
		rows[i] = makeFilterRow(i)
	}

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[filterRow](&buf,
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(50_000), // 20 row groups
	)
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()

	b.Run("full-decode", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			if _, err := parquetfast.UnmarshalBytes[filterRow](data); err != nil {
				b.Fatal(err)
			}
		}
	})

	// Selective: matches ~one row group out of 20.
	b.Run("filtered-1-of-20-groups", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := parquetfast.UnmarshalBytes[filterRow](data,
				parquetfast.Where(parquetfast.Col("id").Between(int64(100_000), int64(140_000))))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Keeps ~half the row groups, so concurrency can parallelize the survivors.
	b.Run("filtered-half-sequential", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := parquetfast.UnmarshalBytes[filterRow](data,
				parquetfast.Where(parquetfast.Col("id").GreaterOrEqual(int64(500_000))))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("filtered-half-concurrent", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := parquetfast.UnmarshalBytes[filterRow](data,
				parquetfast.Where(parquetfast.Col("id").GreaterOrEqual(int64(500_000))),
				parquetfast.WithConcurrency(0))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestFilter_ConcurrentMatchesSequential(t *testing.T) {
	// Multi-row-group file so concurrency parallelizes across groups.
	rows := make([]filterRow, 4000)
	for i := range rows {
		rows[i] = makeFilterRow(i)
	}

	data := writeGeneric(t, rows, parquet.MaxRowsPerRowGroup(100)) // 40 row groups

	preds := []parquetfast.Predicate{
		parquetfast.Col("id").Between(int64(250), int64(3750)),
		parquetfast.Col("region").Equal("eu"),
	}

	seq, err := parquetfast.UnmarshalBytes[filterRow](data, parquetfast.Where(preds...))
	if err != nil {
		t.Fatalf("sequential: %v", err)
	}

	for _, workers := range []int{2, 4, 0 /* GOMAXPROCS */} {
		con, err := parquetfast.UnmarshalBytes[filterRow](data,
			parquetfast.Where(preds...), parquetfast.WithConcurrency(workers))
		if err != nil {
			t.Fatalf("concurrent(%d): %v", workers, err)
		}

		if len(con) != len(seq) {
			t.Fatalf("concurrent(%d): count %d, want %d", workers, len(con), len(seq))
		}

		for i := range seq {
			if con[i] != seq[i] { // file order must be preserved
				t.Fatalf("concurrent(%d) row %d: got %+v want %+v", workers, i, con[i], seq[i])
			}
		}
	}
}

func TestFilter_Or(t *testing.T) {
	data := filterFixture(t, 300, 50)

	assertFilter(t, data, func(r filterRow) bool { return r.ID < 10 || r.ID > 290 },
		parquetfast.Or(
			parquetfast.Col("id").Less(int64(10)),
			parquetfast.Col("id").Greater(int64(290)),
		))

	// OR across different columns.
	assertFilter(t, data, func(r filterRow) bool { return r.Region == "eu" || r.ID < 5 },
		parquetfast.Or(
			parquetfast.Col("region").Equal("eu"),
			parquetfast.Col("id").Less(int64(5)),
		))
}

func TestFilter_NestedAndOr(t *testing.T) {
	data := filterFixture(t, 300, 50)

	// region == "eu" AND (id < 50 OR id > 250)
	assertFilter(t, data,
		func(r filterRow) bool { return r.Region == "eu" && (r.ID < 50 || r.ID > 250) },
		parquetfast.And(
			parquetfast.Col("region").Equal("eu"),
			parquetfast.Or(
				parquetfast.Col("id").Less(int64(50)),
				parquetfast.Col("id").Greater(int64(250)),
			),
		))
}

func TestFilter_OrPrunesRowGroups(t *testing.T) {
	data := filterFixture(t, 2000, 100) // 20 row groups, ids [g*100,(g+1)*100)

	f, _ := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	cc := f.RowGroups()[0].ColumnChunks()[0].(*parquet.FileColumnChunk)
	if _, _, ok := cc.Bounds(); !ok {
		t.Skip("no column statistics")
	}

	read := func(opts ...parquetfast.Option) int64 {
		cr := &countingReaderAt{r: bytes.NewReader(data)}
		if _, err := parquetfast.Unmarshal[filterRow](cr, int64(len(data)), opts...); err != nil {
			t.Fatalf("decode: %v", err)
		}

		return cr.bytes.Load()
	}

	// Matches only group 1 (100..200) or group 18 (1800..1900): 18/20 pruned.
	or := parquetfast.Where(parquetfast.Or(
		parquetfast.Col("id").Between(int64(150), int64(160)),
		parquetfast.Col("id").Between(int64(1850), int64(1860)),
	))

	full := read()
	filtered := read(or)

	t.Logf("OR pruning bytes: full=%d, filtered=%d (%.1f%%)", full, filtered, 100*float64(filtered)/float64(full))

	if filtered >= full {
		t.Fatalf("OR should prune row groups: %d >= %d", filtered, full)
	}

	got, _ := parquetfast.UnmarshalBytes[filterRow](data, or)
	if len(got) != 22 {
		t.Fatalf("expected 22 rows, got %d", len(got))
	}

	for _, r := range got {
		in1 := r.ID >= 150 && r.ID <= 160
		in2 := r.ID >= 1850 && r.ID <= 1860
		if !in1 && !in2 {
			t.Fatalf("row out of OR range: %d", r.ID)
		}
	}
}
