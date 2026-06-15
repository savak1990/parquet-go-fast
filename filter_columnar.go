package parquetfast

import (
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Columnar predicate filtering — "late materialization" for scalar-only output
// with numeric predicates on columns that are also output fields.
//
// Instead of decoding every selected column per row (boxed) and testing the
// predicate per row, it decodes the output columns once via the typed columnar
// path (decodeColumnarRG), then evaluates the predicate straight from the decoded
// structs and keeps the matching rows. Decoding the output once and evaluating in
// place means the filter columns are never read twice.
//
// It applies only when every predicate leaf is a numeric, signed, null-free
// column that is also an output field (so its value can be read back from the
// struct unambiguously). Anything else — strings/bytes/time/bool/unsigned leaves,
// columns with nulls, or filter-only columns not in the struct — falls back to
// the row path, so results are identical to the boxed filter.

// numCmp builds the per-value test for one numeric comparison leaf.
func numCmp[S srcNumeric](op predOp, lo, hi S) func(S) bool {
	switch op {
	case opEq:
		return func(v S) bool { return v == lo }
	case opNe:
		return func(v S) bool { return v != lo }
	case opLt:
		return func(v S) bool { return v < lo }
	case opLe:
		return func(v S) bool { return v <= lo }
	case opGt:
		return func(v S) bool { return v > lo }
	case opGe:
		return func(v S) bool { return v >= lo }
	case opBetween:
		return func(v S) bool { return v >= lo && v <= hi }
	}

	return nil
}

// signedInt reports whether t is a signed integer column. Go's signed comparison
// would be wrong for unsigned values ≥ 2^63 / 2^31, so unsigned columns fall back
// to the row path. A nil/absent Integer logical type is the default signed
// INT32/INT64 (and floats, which have no Integer logical type).
func signedInt(t parquet.Type) bool {
	if lt := t.LogicalType(); lt != nil && lt.Integer != nil {
		return lt.Integer.IsSigned
	}

	return true
}

// outputSetter returns the scalar setter binding leaf column col to a struct
// field, or nil if the column isn't an output field.
func outputSetter(plan *Plan, col int) *scalarSetter {
	for i := range plan.scalars {
		if int(plan.scalars[i].col) == col {
			return &plan.scalars[i]
		}
	}

	return nil
}

// structEvalKind reports whether a field of this kind can be read back from the
// decoded struct for predicate evaluation: numeric, signed, and with the Go field
// type equal to the physical type (narrow/unsigned ints fall back).
func structEvalKind(kind setterKind, typ parquet.Type) bool {
	switch kind {
	case kindInt32, kindInt64, kindInt, kindFloat32, kindFloat64:
		return signedInt(typ)
	}

	return false
}

// canEvalFromStruct reports whether the predicate can be evaluated from the
// decoded structs: every leaf is a struct-readable numeric field whose column has
// no nulls in this row group (a null would decode to the zero value, which the
// predicate couldn't distinguish from a real zero).
func canEvalFromStruct(rg parquet.RowGroup, plan *Plan, cp *compiledPredicate) bool {
	switch cp.kind {
	case predLeaf:
		s := outputSetter(plan, cp.col)
		if s == nil || !structEvalKind(s.kind, cp.typ) {
			return false
		}

		if cp.col < 0 || cp.col >= len(rg.ColumnChunks()) {
			return false
		}

		fcc, ok := rg.ColumnChunks()[cp.col].(*parquet.FileColumnChunk)

		return ok && fcc.NullCount() == 0
	case predAnd, predOr:
		for i := range cp.children {
			if !canEvalFromStruct(rg, plan, &cp.children[i]) {
				return false
			}
		}

		return true
	}

	return false
}

// evalMaskStruct evaluates the (normalized, Not-free) predicate tree against the
// decoded structs, returning a per-row match bitmap.
func evalMaskStruct[T any](tmp []T, plan *Plan, cp *compiledPredicate) []bool {
	switch cp.kind {
	case predLeaf:
		return leafMaskStruct(tmp, outputSetter(plan, cp.col), cp)
	case predAnd:
		var acc []bool

		for i := range cp.children {
			m := evalMaskStruct(tmp, plan, &cp.children[i])
			if acc == nil {
				acc = m
			} else {
				for j := range acc {
					acc[j] = acc[j] && m[j]
				}
			}
		}

		return acc
	case predOr:
		var acc []bool

		for i := range cp.children {
			m := evalMaskStruct(tmp, plan, &cp.children[i])
			if acc == nil {
				acc = m
			} else {
				for j := range acc {
					acc[j] = acc[j] || m[j]
				}
			}
		}

		return acc
	}

	return nil
}

// leafMaskStruct evaluates one numeric leaf by reading the bound field out of each
// decoded struct via its offset, with no parquet.Value boxing.
func leafMaskStruct[T any](tmp []T, s *scalarSetter, cp *compiledPredicate) []bool {
	n := len(tmp)
	mask := make([]bool, n)

	if n == 0 {
		return mask
	}

	base := unsafe.Pointer(&tmp[0])
	esz := unsafe.Sizeof(tmp[0])
	off := s.offset

	switch s.kind {
	case kindInt32:
		var hi int32
		if cp.op == opBetween {
			hi = cp.hi.Int32()
		}

		cmp := numCmp(cp.op, cp.lo.Int32(), hi)
		for i := 0; i < n; i++ {
			mask[i] = cmp(*(*int32)(unsafe.Add(base, uintptr(i)*esz+off)))
		}
	case kindInt64:
		var hi int64
		if cp.op == opBetween {
			hi = cp.hi.Int64()
		}

		cmp := numCmp(cp.op, cp.lo.Int64(), hi)
		for i := 0; i < n; i++ {
			mask[i] = cmp(*(*int64)(unsafe.Add(base, uintptr(i)*esz+off)))
		}
	case kindInt:
		var hi int64
		if cp.op == opBetween {
			hi = cp.hi.Int64()
		}

		cmp := numCmp(cp.op, cp.lo.Int64(), hi)
		for i := 0; i < n; i++ {
			mask[i] = cmp(int64(*(*int)(unsafe.Add(base, uintptr(i)*esz+off))))
		}
	case kindFloat32:
		var hi float32
		if cp.op == opBetween {
			hi = cp.hi.Float()
		}

		cmp := numCmp(cp.op, cp.lo.Float(), hi)
		for i := 0; i < n; i++ {
			mask[i] = cmp(*(*float32)(unsafe.Add(base, uintptr(i)*esz+off)))
		}
	case kindFloat64:
		var hi float64
		if cp.op == opBetween {
			hi = cp.hi.Double()
		}

		cmp := numCmp(cp.op, cp.lo.Double(), hi)
		for i := 0; i < n; i++ {
			mask[i] = cmp(*(*float64)(unsafe.Add(base, uintptr(i)*esz+off)))
		}
	}

	return mask
}

// filterGroupColumnar decodes the output columns once (typed), evaluates the
// predicate from the decoded structs, and keeps matching rows. handled=false
// means the predicate isn't struct-evaluable and the caller should use the row
// path. Requires plan.scalarOnly().
func filterGroupColumnar[T any](rg parquet.RowGroup, plan *Plan, root *compiledPredicate, n int) (result []T, handled bool, err error) {
	if !canEvalFromStruct(rg, plan, root) {
		return nil, false, nil
	}

	tmp := make([]T, n)
	if derr := decodeColumnarRG(rg, plan, tmp, 0, make([]parquet.Value, columnarBatch)); derr != nil {
		return nil, true, derr
	}

	mask := evalMaskStruct(tmp, plan, root)

	matches := 0

	for _, m := range mask {
		if m {
			matches++
		}
	}

	out := make([]T, 0, matches)

	for i := 0; i < n; i++ {
		if mask[i] {
			out = append(out, tmp[i])
		}
	}

	return out, true, nil
}
