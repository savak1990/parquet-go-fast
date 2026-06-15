package parquetfast

import (
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

// Columnar decode, Tier 2 — typed dict-gather for scalar numeric columns.
//
// Tier 1 reads each column as boxed parquet.Value (one 24-byte struct per value,
// plus an enum-switch unbox in applyScalar). Tier 2 reads the page's typed buffer
// directly (encoding.Values.Int32()/Int64()/Float()/Double()), resolves
// dictionary indices against the dictionary's typed value buffer, and writes the
// destination field with a single typed store — no parquet.Value, no per-value
// switch. Definition levels place nulls (the typed buffer is dense, non-null).
//
// Only the numeric scalar kinds are handled here; strings, bools, []byte,
// time.Time, and optional (*T) kinds fall back to the Tier-1 boxed page loop, as
// do any pages whose physical encoding.Kind doesn't match the expectation.

type srcNumeric interface {
	int32 | int64 | float32 | float64
}

type dstNumeric interface {
	int | int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// gatherColumn writes one page's worth of a numeric column into the destination
// structs. Source values are read from dense (plain pages) or dict[indices[i]]
// (dictionary pages); a non-empty defLevels places nulls (def 0 → skip, leaving
// the Go zero value). All writes are typed (D(v)); no boxing, no closures.
func gatherColumn[S srcNumeric, D dstNumeric](
	base0 unsafe.Pointer, elemSize, offset uintptr, rowBase int, row *int,
	defLevels []byte, dense, dict []S, indices []int32,
) {
	r := *row

	switch {
	case dict != nil && len(defLevels) == 0: // dictionary, required
		for _, idx := range indices {
			*(*D)(unsafe.Add(base0, uintptr(rowBase+r)*elemSize+offset)) = D(dict[idx])
			r++
		}
	case dict != nil: // dictionary, optional
		di := 0

		for _, d := range defLevels {
			if d != 0 {
				*(*D)(unsafe.Add(base0, uintptr(rowBase+r)*elemSize+offset)) = D(dict[indices[di]])
				di++
			}

			r++
		}
	case len(defLevels) == 0: // plain, required
		for i := range dense {
			*(*D)(unsafe.Add(base0, uintptr(rowBase+r)*elemSize+offset)) = D(dense[i])
			r++
		}
	default: // plain, optional
		di := 0

		for _, d := range defLevels {
			if d != 0 {
				*(*D)(unsafe.Add(base0, uintptr(rowBase+r)*elemSize+offset)) = D(dense[di])
				di++
			}

			r++
		}
	}

	*row = r
}

// applyTypedPage decodes one page of a numeric scalar column directly from its
// typed buffer, advancing *row by the page's row count. Returns false (caller
// falls back to the boxed path) for non-numeric kinds or unexpected encodings.
func applyTypedPage(pg parquet.Page, s *scalarSetter, base0 unsafe.Pointer, elemSize uintptr, rowBase, n int, row *int) bool {
	var phys encoding.Kind

	switch s.kind {
	case kindInt32, kindInt8, kindInt16, kindUint8, kindUint16, kindUint32:
		phys = encoding.Int32
	case kindInt64, kindInt, kindUint64:
		phys = encoding.Int64
	case kindFloat32:
		phys = encoding.Float
	case kindFloat64:
		phys = encoding.Double
	default:
		return false // string/bool/bytes/time/optional → boxed path
	}

	defLevels := pg.DefinitionLevels()
	data := pg.Data()

	var (
		indices []int32
		dictV   encoding.Values
		dict    bool
	)

	if dp := pg.Dictionary(); dp != nil {
		if data.Kind() != encoding.Int32 {
			return false
		}

		indices = data.Int32()
		dictV = dp.Page().Data()

		if dictV.Kind() != phys {
			return false
		}

		dict = true
	} else if data.Kind() != phys {
		return false
	}

	// Rows represented by this page (one def level per row; the typed buffer /
	// indices are dense over the non-null rows).
	pageRows := len(defLevels)
	if pageRows == 0 {
		if dict {
			pageRows = len(indices)
		} else {
			bytesPerValue := 4
			if phys == encoding.Int64 || phys == encoding.Double {
				bytesPerValue = 8
			}

			pageRows = int(data.Size()) / bytesPerValue
		}
	}

	// Guard against a page claiming more rows than the group reports (corruption);
	// the boxed fallback bounds-checks per value.
	if *row+pageRows > n {
		return false
	}

	switch phys {
	case encoding.Int32:
		var dense, dvals []int32
		if dict {
			dvals = dictV.Int32()
		} else {
			dense = data.Int32()
		}

		dispatchInt32(s.kind, base0, elemSize, s.offset, rowBase, row, defLevels, dense, dvals, indices)
	case encoding.Int64:
		var dense, dvals []int64
		if dict {
			dvals = dictV.Int64()
		} else {
			dense = data.Int64()
		}

		dispatchInt64(s.kind, base0, elemSize, s.offset, rowBase, row, defLevels, dense, dvals, indices)
	case encoding.Float:
		var dense, dvals []float32
		if dict {
			dvals = dictV.Float()
		} else {
			dense = data.Float()
		}

		gatherColumn[float32, float32](base0, elemSize, s.offset, rowBase, row, defLevels, dense, dvals, indices)
	case encoding.Double:
		var dense, dvals []float64
		if dict {
			dvals = dictV.Double()
		} else {
			dense = data.Double()
		}

		gatherColumn[float64, float64](base0, elemSize, s.offset, rowBase, row, defLevels, dense, dvals, indices)
	}

	return true
}

func dispatchInt32(kind setterKind, base0 unsafe.Pointer, elemSize, offset uintptr, rowBase int, row *int, defLevels []byte, dense, dict, indices []int32) {
	switch kind {
	case kindInt32:
		gatherColumn[int32, int32](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	case kindUint32:
		gatherColumn[int32, uint32](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	case kindInt8:
		gatherColumn[int32, int8](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	case kindInt16:
		gatherColumn[int32, int16](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	case kindUint8:
		gatherColumn[int32, uint8](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	case kindUint16:
		gatherColumn[int32, uint16](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	}
}

func dispatchInt64(kind setterKind, base0 unsafe.Pointer, elemSize, offset uintptr, rowBase int, row *int, defLevels []byte, dense, dict []int64, indices []int32) {
	switch kind {
	case kindInt64:
		gatherColumn[int64, int64](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	case kindInt:
		gatherColumn[int64, int](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	case kindUint64:
		gatherColumn[int64, uint64](base0, elemSize, offset, rowBase, row, defLevels, dense, dict, indices)
	}
}
