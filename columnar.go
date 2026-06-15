package parquetfast

import (
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Columnar decode (Tier 1) — for plans that bind only scalar / optional-scalar
// leaves (no maps, lists, or optional structs). Instead of routing every value
// through parquet-go's row reader (which assembles each row into a []parquet.Value
// and forces us to re-scatter it by column in unflattenRow), we read each bound
// column's chunk in bulk and write it strided into the destination structs. This
// removes the row-assembly and re-scatter passes that dominate the row path; the
// per-value typed write (applyScalar) is unchanged.
//
// Columns are still read as parquet.Value (parquet-go decodes dictionary/encoding
// and applies definition levels, so nulls arrive as null Values) — Tier 1 keeps
// that boxing; only the loop order changes from row-major to column-major.
//
// Eligibility is gated by plan.scalarOnly(); any compound field falls back to the
// row path, so nested schemas are unaffected.

const columnarBatch = 1024

// scalarOnly reports whether the plan can use the columnar path: it binds at
// least one leaf and every binding is a scalar setter (no compound handlers).
func (p *Plan) scalarOnly() bool {
	return len(p.compound) == 0 && len(p.scalars) > 0
}

// decodeColumnar decodes every row group into out (zero-valued, sized to the
// total row count) column-by-column, in row-group order. batch is reusable
// scratch for value reads.
func decodeColumnar[T any](rgs []parquet.RowGroup, plan *Plan, out []T, batch []parquet.Value) error {
	rowBase := 0

	for _, rg := range rgs {
		n := int(rg.NumRows())
		if n > 0 {
			if err := decodeColumnarRG(rg, plan, out, rowBase, batch); err != nil {
				return err
			}
		}

		rowBase += n
	}

	return nil
}

// decodeColumnarRG decodes one row group's bound scalar columns into
// out[rowBase : rowBase+rg.NumRows()]. out must be non-empty (its first element's
// address anchors the strided writes).
func decodeColumnarRG[T any](rg parquet.RowGroup, plan *Plan, out []T, rowBase int, batch []parquet.Value) error {
	var zero T

	elemSize := unsafe.Sizeof(zero)
	base0 := unsafe.Pointer(&out[0])
	n := int(rg.NumRows())
	chunks := rg.ColumnChunks()

	for si := range plan.scalars {
		s := &plan.scalars[si]
		col := int(s.col)

		// Skip columns the plan doesn't read here: out of range, or proven
		// 100% null (the field keeps its zero value, matching the row path).
		if col < 0 || col >= len(chunks) || plan.isSkipped(col) {
			continue
		}

		if err := applyColumn(chunks[col], s, base0, elemSize, rowBase, n, batch); err != nil {
			return fmt.Errorf("decode column %d: %w", col, err)
		}
	}

	return nil
}

// applyColumn reads every value of one column chunk (page by page) and writes it
// into the matching field of out[rowBase + i] through the scalar setter.
func applyColumn(chunk parquet.ColumnChunk, s *scalarSetter, base0 unsafe.Pointer, elemSize uintptr, rowBase, n int, batch []parquet.Value) (err error) {
	pages := chunk.Pages()

	defer func() {
		if cerr := pages.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	row := 0

	for {
		pg, perr := pages.ReadPage()
		if perr != nil {
			if errors.Is(perr, io.EOF) {
				break
			}

			return perr
		}

		// Tier 2: decode numeric columns straight from the typed buffer (no
		// parquet.Value boxing). Falls back to the boxed reader for kinds /
		// encodings it doesn't handle (strings, bools, time, optionals, …).
		var rerr error
		if !applyTypedPage(pg, s, base0, elemSize, rowBase, n, &row) {
			rerr = applyPageValues(pg.Values(), s, base0, elemSize, rowBase, n, &row, batch)
		}

		parquet.Release(pg)

		if rerr != nil {
			return rerr
		}
	}

	if row != n {
		return fmt.Errorf("column decoded %d values, want %d", row, n)
	}

	return nil
}

// applyPageValues drains one page's value reader into the destination column,
// advancing *row. It guards against a column yielding more values than the row
// group claims (corruption), which would otherwise write out of bounds.
func applyPageValues(vr parquet.ValueReader, s *scalarSetter, base0 unsafe.Pointer, elemSize uintptr, rowBase, n int, row *int, batch []parquet.Value) error {
	for {
		m, rerr := vr.ReadValues(batch)

		for j := 0; j < m; j++ {
			if *row >= n {
				return fmt.Errorf("column yielded more than %d values", n)
			}

			elemPtr := unsafe.Add(base0, uintptr(rowBase+*row)*elemSize)
			applyScalar(s.kind, unsafe.Add(elemPtr, s.offset), batch[j])
			*row++
		}

		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}

			return rerr
		}
	}
}
