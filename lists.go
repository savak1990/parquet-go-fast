package parquetfast

import (
	"reflect"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Lists: Go fields of kind reflect.Slice. Two flavours:
//
//   - Primitive lists ([]string, []int64, []float64, []bool, …) —
//     addPrimitiveList. Typed loop, no reflect on the hot path.
//   - Struct lists ([]Struct) — addStructList. Typed fast path when the element
//     type is registered via RegisterStructList; reflect fallback otherwise.
//
// []byte goes through addScalarLeaf with kindBytes (BYTE_ARRAY scalar, not a
// list) — handled in addFieldByKind before this file is reached.

// listShape resolves a Go slice field's element-subtree prefix and the path of
// its repeated node, handling both layouts parquet-go can write:
//
//   - default `repeated` layout — path itself is the repeated node, elements
//     live directly under it (column path == path for a primitive slice).
//   - 3-level LIST logical layout (emitted for a `,list`-tagged field) —
//     path/list/element, with the repeated node at path/list.
func listShape(schema *parquet.Schema, path []string) (elemPrefix, repPath []string, ok bool) {
	listPrefix := appendPath(path, pqtListSeg, pqtElementSeg)
	if anyColumnUnder(schema, listPrefix) {
		return listPrefix, appendPath(path, pqtListSeg), true
	}

	if anyColumnUnder(schema, path) {
		return path, path, true
	}

	return nil, nil, false
}

func anyColumnUnder(schema *parquet.Schema, prefix []string) bool {
	for _, p := range schema.Columns() {
		if pathStartsWith(p, prefix) {
			return true
		}
	}

	return false
}

// addPrimitiveList installs a compound for a slice of primitive elements.
func addPrimitiveList(plan *Plan, ft reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	elemKind, ok := scalarKindFor(ft.Elem().Kind())
	if !ok {
		return unsupportedKindErr(ft, path)
	}

	elemPath, _, ok := listShape(schema, path)
	if !ok {
		return nil
	}

	leaf, ok := schema.Lookup(elemPath...)
	if !ok {
		return nil
	}

	if !validColumn(leaf.ColumnIndex, plan.numLeaves) {
		return nil
	}

	col := leaf.ColumnIndex
	plan.markRef(col)

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		vs := leafVals[col]
		if len(vs) == 0 {
			return
		}

		// Parquet emits a single null value for absent/empty lists.
		if len(vs) == 1 && vs[0].IsNull() {
			return
		}

		appendPrimitiveSlice(elemKind, unsafe.Add(base, offset), vs)
	})

	return nil
}

// appendPrimitiveSlice builds a typed slice from vs (skipping nulls) and writes
// it to the field at dst. One arm per primitive element kind.
func appendPrimitiveSlice(kind setterKind, dst unsafe.Pointer, vs []parquet.Value) {
	switch kind {
	case kindString:
		out := make([]string, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, string(vs[i].ByteArray()))
			}
		}

		*(*[]string)(dst) = out
	case kindBool:
		out := make([]bool, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, vs[i].Boolean())
			}
		}

		*(*[]bool)(dst) = out
	case kindInt:
		out := make([]int, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, int(vs[i].Int64()))
			}
		}

		*(*[]int)(dst) = out
	case kindInt8:
		out := make([]int8, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, int8(vs[i].Int32()))
			}
		}

		*(*[]int8)(dst) = out
	case kindInt16:
		out := make([]int16, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, int16(vs[i].Int32()))
			}
		}

		*(*[]int16)(dst) = out
	case kindInt32:
		out := make([]int32, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, vs[i].Int32())
			}
		}

		*(*[]int32)(dst) = out
	case kindInt64:
		out := make([]int64, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, vs[i].Int64())
			}
		}

		*(*[]int64)(dst) = out
	case kindUint16:
		out := make([]uint16, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, uint16(vs[i].Int32()))
			}
		}

		*(*[]uint16)(dst) = out
	case kindUint32:
		out := make([]uint32, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, uint32(vs[i].Int32()))
			}
		}

		*(*[]uint32)(dst) = out
	case kindUint64:
		out := make([]uint64, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, uint64(vs[i].Int64()))
			}
		}

		*(*[]uint64)(dst) = out
	case kindFloat32:
		out := make([]float32, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, vs[i].Float())
			}
		}

		*(*[]float32)(dst) = out
	case kindFloat64:
		out := make([]float64, 0, len(vs))
		for i := range vs {
			if !vs[i].IsNull() {
				out = append(out, vs[i].Double())
			}
		}

		*(*[]float64)(dst) = out
	}
	// kindUint8 ([]uint8) and kindBytes never reach here — []byte is BYTE_ARRAY,
	// routed to a scalar setter in addFieldByKind.
}

// addStructList installs a compound for []Struct. Walks the element subtree once
// at build time, then per-row rep-splits the nested columns and applies the
// sub-plan to each entry. Uses a typed filler when the element type is
// registered via RegisterStructList, else a reflect-based slice build.
func addStructList(plan *Plan, ft reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	elemPrefix, repPath, ok := listShape(schema, path)
	if !ok {
		return nil
	}

	st := ft.Elem()

	sub := plan.newSubPlan()
	if err := addStructFields(sub, st, 0, elemPrefix, schema); err != nil {
		return err
	}

	outerRep := repLevelOfPath(schema, repPath)
	if outerRep < 0 {
		return nil
	}

	numLeaves := plan.numLeaves
	info := collectSubtreeInfo(schema, elemPrefix, numLeaves, outerRep, plan.skipCol)

	// A present struct list reads its whole element subtree (entry counting,
	// rep-splitting, and the sentinel all need it), so mark all of it referenced.
	plan.markRefs(info.scalarCols)
	plan.markRefs(info.nestedCols)

	sentinelCol, ok := pickSentinelCol(info)
	if !ok {
		return nil
	}

	ctx := subtreeIterCtx{info: info, outerRep: outerRep, numLeaves: numLeaves}

	filler, typed := typedStructListFiller(ft)

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		sentinelVs := leafVals[sentinelCol]
		if len(sentinelVs) == 0 {
			return
		}

		if len(sentinelVs) == 1 && sentinelVs[0].IsNull() {
			return
		}

		n := len(splitByRep(sentinelVs, outerRep))
		if n == 0 {
			return
		}

		slicePtr := unsafe.Add(base, offset)

		if typed {
			filler(slicePtr, n, sub, ctx, leafVals)

			return
		}

		// Reflect fallback for unregistered element types.
		sliceVal := reflect.MakeSlice(ft, n, n)
		applyPerEntrySubPlan(leafVals, ctx, n, func(buf [][]parquet.Value, i int) {
			sub.applyDecoded(sliceVal.Index(i).Addr().UnsafePointer(), buf)
		})

		reflect.NewAt(ft, slicePtr).Elem().Set(sliceVal)
	})

	return nil
}

// pickSentinelCol prefers any scalar column as the entry-count sentinel.
func pickSentinelCol(info subtreeInfo) (int, bool) {
	if len(info.scalarCols) > 0 {
		return info.scalarCols[0], true
	}

	if len(info.nestedCols) > 0 {
		return info.nestedCols[0], true
	}

	return 0, false
}
