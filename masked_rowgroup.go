package parquetfast

import (
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

// maskedRowGroup wraps a parquet.RowGroup and substitutes nullColumnChunk stubs
// for columns identified as 100% null across the file. The stubs skip
// parquet-go's read pipeline for those columns (page fetch, decompression,
// RLE/dict decoding) since every value is known to be null.
//
// Schema and NumRows pass through unchanged, so the leaf column indices the plan
// captured at build time remain valid. This composes with parquet.MultiRowGroup:
// mask each underlying row group first, then combine.
type maskedRowGroup struct {
	inner  parquet.RowGroup
	chunks []parquet.ColumnChunk
}

// NewMaskedRowGroup wraps rg, replacing each column chunk i where skip[i] is true
// with a null-stub. Returns the original row group untouched (zero allocations)
// when skip is empty or has no true entries.
func NewMaskedRowGroup(rg parquet.RowGroup, skip []bool) parquet.RowGroup {
	if !anyTrue(skip) {
		return rg
	}

	orig := rg.ColumnChunks()
	chunks := make([]parquet.ColumnChunk, len(orig))
	numRows := rg.NumRows()

	for i, c := range orig {
		if i < len(skip) && skip[i] {
			chunks[i] = &nullColumnChunk{inner: c, numRows: numRows}
		} else {
			chunks[i] = c
		}
	}

	return &maskedRowGroup{inner: rg, chunks: chunks}
}

func (m *maskedRowGroup) NumRows() int64 {
	return m.inner.NumRows()
}

func (m *maskedRowGroup) ColumnChunks() []parquet.ColumnChunk {
	return m.chunks
}

func (m *maskedRowGroup) Schema() *parquet.Schema {
	return m.inner.Schema()
}

func (m *maskedRowGroup) SortingColumns() []parquet.SortingColumn {
	return m.inner.SortingColumns()
}

func (m *maskedRowGroup) Rows() parquet.Rows {
	return parquet.NewRowGroupRowReader(m)
}

// nullColumnChunk yields N null values via an in-memory stream, skipping any
// IO/decode. Delegates non-data methods to inner so callers that inspect the
// schema/type/column-index still see the real column metadata.
type nullColumnChunk struct {
	inner   parquet.ColumnChunk
	numRows int64
}

func (c *nullColumnChunk) Type() parquet.Type {
	return c.inner.Type()
}

func (c *nullColumnChunk) Column() int {
	return c.inner.Column()
}

func (c *nullColumnChunk) ColumnIndex() (parquet.ColumnIndex, error) {
	return c.inner.ColumnIndex()
}

func (c *nullColumnChunk) OffsetIndex() (parquet.OffsetIndex, error) {
	return c.inner.OffsetIndex()
}

func (*nullColumnChunk) BloomFilter() parquet.BloomFilter {
	return nil
}

func (c *nullColumnChunk) NumValues() int64 {
	return c.numRows
}

func (c *nullColumnChunk) Pages() parquet.Pages {
	return &nullPages{n: c.numRows, col: c.inner.Column(), typ: c.inner.Type()}
}

// nullPages emits the remaining null values (n - offset) as a single page, then
// EOF. offset is advanced by SeekToRow so partial seeks return only the values
// from the seek point onward — matches parquet-go's Pages contract.
type nullPages struct {
	n         int64
	col       int
	typ       parquet.Type
	offset    int64
	delivered bool
}

func (p *nullPages) ReadPage() (parquet.Page, error) {
	if p.delivered || p.offset >= p.n {
		return nil, io.EOF
	}

	p.delivered = true
	remaining := p.n - p.offset

	return &nullPage{n: remaining, col: p.col, typ: p.typ}, nil
}

func (p *nullPages) SeekToRow(row int64) error {
	p.offset = max(0, min(row, p.n))
	p.delivered = false

	return nil
}

func (*nullPages) Close() error {
	return nil
}

// nullPage represents N null values on one column. Only Values() is read on the
// row-assembly hot path; the rest of the Page interface is stubbed.
type nullPage struct {
	n   int64
	col int
	typ parquet.Type
}

func (p *nullPage) Type() parquet.Type {
	return p.typ
}

func (p *nullPage) Column() int {
	return p.col
}

func (*nullPage) Dictionary() parquet.Dictionary {
	return nil
}

func (p *nullPage) NumRows() int64 {
	return p.n
}

func (p *nullPage) NumValues() int64 {
	return p.n
}

func (p *nullPage) NumNulls() int64 {
	return p.n
}

//nolint:revive // function-result-limit (parquet.Page interface)
func (*nullPage) Bounds() (minV, maxV parquet.Value, ok bool) {
	return parquet.Value{}, parquet.Value{}, false
}

func (*nullPage) Size() int64 {
	return 0
}

func (*nullPage) RepetitionLevels() []byte {
	return nil
}

func (*nullPage) DefinitionLevels() []byte {
	return nil
}

func (*nullPage) Data() encoding.Values {
	return encoding.Values{}
}

func (p *nullPage) Slice(i, j int64) parquet.Page {
	return &nullPage{n: j - i, col: p.col, typ: p.typ}
}

func (p *nullPage) Values() parquet.ValueReader {
	return &nullValueReader{remaining: p.n, col: p.col}
}

// nullValueReader streams N null Values stamped with their column index, so the
// bucketing-by-Value.Column() loop routes them correctly.
type nullValueReader struct {
	remaining int64
	col       int
}

func (r *nullValueReader) ReadValues(values []parquet.Value) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}

	n := min(int64(len(values)), r.remaining)

	nullVal := parquet.NullValue().Level(0, 0, r.col)
	for i := range values[:n] {
		values[i] = nullVal
	}

	r.remaining -= n
	if r.remaining == 0 {
		return int(n), io.EOF
	}

	return int(n), nil
}
