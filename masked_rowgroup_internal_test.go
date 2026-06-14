package parquetfast

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// These stub methods satisfy parquet-go's RowGroup/ColumnChunk/Page interfaces
// but the row-assembly path only ever calls Values()/ReadPage(); the rest are
// never reached through an integration decode. Exercise them directly so the
// null-column-skip stubs are covered and their contracts are pinned.

type maskStubRow struct {
	A int64   `parquet:"a"`
	B *string `parquet:"b,optional"` // never set → a 100%-null column
}

func TestMaskedRowGroup_StubMethods(t *testing.T) {
	t.Parallel()

	rows := []maskStubRow{{A: 1}, {A: 2}, {A: 3}}

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[maskStubRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()

	skip := allNullCols(rgs, f.Schema())
	if !anyTrue(skip) {
		t.Fatal("expected the optional column to be detected as all-null")
	}

	m := NewMaskedRowGroup(rgs[0], skip)

	// maskedRowGroup passthroughs.
	if m.NumRows() != 3 {
		t.Fatalf("NumRows = %d, want 3", m.NumRows())
	}

	if m.Schema() == nil {
		t.Fatal("Schema() = nil")
	}

	_ = m.SortingColumns() // delegates to inner; just exercise it.

	mrg, ok := m.(*maskedRowGroup)
	if !ok {
		t.Fatalf("NewMaskedRowGroup returned %T, want *maskedRowGroup", m)
	}

	var nc *nullColumnChunk

	for _, c := range mrg.ColumnChunks() {
		if x, isNull := c.(*nullColumnChunk); isNull {
			nc = x
		}
	}

	if nc == nil {
		t.Fatal("no nullColumnChunk among ColumnChunks")
	}

	// nullColumnChunk stubs.
	if nc.Type() == nil {
		t.Fatal("Type() = nil")
	}

	if _, err := nc.ColumnIndex(); err != nil {
		t.Fatalf("ColumnIndex: %v", err)
	}

	if _, err := nc.OffsetIndex(); err != nil {
		t.Fatalf("OffsetIndex: %v", err)
	}

	if nc.BloomFilter() != nil {
		t.Fatal("BloomFilter() should be nil for a null column")
	}

	if nc.NumValues() != 3 {
		t.Fatalf("NumValues = %d, want 3", nc.NumValues())
	}

	col := nc.Column()

	// nullPages / nullPage stubs.
	pages := nc.Pages()
	defer func() { _ = pages.Close() }()

	pg, err := pages.ReadPage()
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}

	if pg.Type() == nil {
		t.Fatal("page Type() = nil")
	}

	if pg.Column() != col {
		t.Fatalf("page Column() = %d, want %d", pg.Column(), col)
	}

	if pg.Dictionary() != nil {
		t.Fatal("page Dictionary() should be nil")
	}

	if pg.NumRows() != 3 || pg.NumValues() != 3 || pg.NumNulls() != 3 {
		t.Fatalf("page counts: rows=%d values=%d nulls=%d", pg.NumRows(), pg.NumValues(), pg.NumNulls())
	}

	if _, _, ok := pg.Bounds(); ok {
		t.Fatal("Bounds() ok should be false for a null page")
	}

	if pg.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", pg.Size())
	}

	if pg.RepetitionLevels() != nil || pg.DefinitionLevels() != nil {
		t.Fatal("levels should be nil")
	}

	_ = pg.Data()

	if sl := pg.Slice(0, 2); sl.NumValues() != 2 {
		t.Fatalf("Slice(0,2).NumValues() = %d, want 2", sl.NumValues())
	}

	// Values() emits exactly n nulls then io.EOF.
	vr := pg.Values()

	out := make([]parquet.Value, 10)

	n, err := vr.ReadValues(out)
	if n != 3 || !errors.Is(err, io.EOF) {
		t.Fatalf("ReadValues = (%d, %v), want (3, EOF)", n, err)
	}

	for i := 0; i < n; i++ {
		if !out[i].IsNull() {
			t.Fatalf("value %d not null", i)
		}

		if out[i].Column() != col {
			t.Fatalf("value %d column = %d, want %d", i, out[i].Column(), col)
		}
	}

	if n, err := vr.ReadValues(out); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("drained ReadValues = (%d, %v), want (0, EOF)", n, err)
	}

	// SeekToRow rewinds the page stream and trims the leading rows.
	if err := pages.SeekToRow(1); err != nil {
		t.Fatalf("SeekToRow: %v", err)
	}

	pg2, err := pages.ReadPage()
	if err != nil {
		t.Fatalf("ReadPage after seek: %v", err)
	}

	if pg2.NumValues() != 2 {
		t.Fatalf("after SeekToRow(1): NumValues = %d, want 2", pg2.NumValues())
	}

	// Seeking past the end yields no more pages.
	if err := pages.SeekToRow(3); err != nil {
		t.Fatalf("SeekToRow(end): %v", err)
	}

	if _, err := pages.ReadPage(); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadPage past end = %v, want EOF", err)
	}
}
