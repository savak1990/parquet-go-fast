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
// its repeated node, handling the layouts producers emit:
//
//   - default `repeated` layout — path itself is the repeated node, elements
//     live directly under it (column path == path for a primitive slice).
//   - 3-level LIST logical layout — path/<repeated>/<element>. The repeated and
//     element names vary by producer (element/item/array, list/bag), so they are
//     resolved structurally from the schema tree, not by the spec-default name.
//     parquet-go's own GenericReader assumes "element" and silently reads empty
//     when a file (e.g. parquet-cpp output) names it "item"; resolving by
//     structure decodes those files correctly.
func listShape(schema *parquet.Schema, path []string) (elemPrefix, repPath []string, ok bool) {
	if node, found := nodeAt(schema, path); found {
		// Default repeated layout: the field is the repeated node itself.
		if node.Repeated() {
			return path, path, true
		}

		// 3-level LIST: <group>(LIST) { repeated <rep> { <element> } }.
		if fields := node.Fields(); len(fields) == 1 && fields[0].Repeated() {
			rep := fields[0]
			repP := appendPath(path, rep.Name())

			if elem := rep.Fields(); len(elem) == 1 {
				return appendPath(repP, elem[0].Name()), repP, true
			}
		}
	}

	// Fallback to path probing for schemas we can't navigate by field name.
	if std := appendPath(path, pqtListSeg, pqtElementSeg); anyColumnUnder(schema, std) {
		return std, appendPath(path, pqtListSeg), true
	}

	if anyColumnUnder(schema, path) {
		return path, path, true
	}

	return nil, nil, false
}

// nodeAt walks the schema tree along path (by field name) and returns the node
// found there.
func nodeAt(schema *parquet.Schema, path []string) (parquet.Node, bool) {
	var n parquet.Node = schema

	for _, seg := range path {
		child, ok := childByName(n, seg)
		if !ok {
			return nil, false
		}

		n = child
	}

	return n, true
}

func childByName(n parquet.Node, name string) (parquet.Field, bool) {
	for _, f := range n.Fields() {
		if f.Name() == name {
			return f, true
		}
	}

	return nil, false
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

// addOptionalPrimitiveList installs a compound for a slice of pointer-to-scalar
// elements ([]*int64, []*string, …) — a list whose elements are nullable. Unlike
// []T (which drops nulls), []*T preserves element positions, mapping a null
// element to a nil pointer. A null or empty list leaves the destination nil.
func addOptionalPrimitiveList(plan *Plan, ft reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	elemKind, ok := scalarKindFor(ft.Elem().Elem().Kind())
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

	// A value is a real element slot only when its definition level reaches the
	// element node. A null or empty list emits a single lower-level placeholder
	// (no slot), which must leave the slice nil rather than produce [nil].
	elemLevel := leaf.MaxDefinitionLevel
	if leaf.Node.Optional() {
		elemLevel--
	}

	plan.markRef(col)

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		vs := leafVals[col]
		if len(vs) == 0 {
			return
		}

		appendPrimitivePtrSlice(elemKind, unsafe.Add(base, offset), vs, elemLevel)
	})

	return nil
}

// appendPrimitivePtrSlice builds a []*T from vs, preserving positions: a value
// below elemLevel is a null/empty-list placeholder (skipped), a null element
// becomes nil, a present element becomes a pointer to its value. The slice is
// written only when at least one real element slot is present, so null/empty
// lists leave the field at its nil zero value.
func appendPrimitivePtrSlice(kind setterKind, dst unsafe.Pointer, vs []parquet.Value, elemLevel int) {
	switch kind {
	case kindString:
		var out []*string

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := string(vs[i].ByteArray())
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*string)(dst) = out
		}
	case kindBool:
		var out []*bool

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := vs[i].Boolean()
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*bool)(dst) = out
		}
	case kindInt:
		var out []*int

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := int(vs[i].Int64())
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*int)(dst) = out
		}
	case kindInt8:
		var out []*int8

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := int8(vs[i].Int32())
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*int8)(dst) = out
		}
	case kindInt16:
		var out []*int16

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := int16(vs[i].Int32())
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*int16)(dst) = out
		}
	case kindInt32:
		var out []*int32

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := vs[i].Int32()
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*int32)(dst) = out
		}
	case kindInt64:
		var out []*int64

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := vs[i].Int64()
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*int64)(dst) = out
		}
	case kindUint16:
		var out []*uint16

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := uint16(vs[i].Int32())
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*uint16)(dst) = out
		}
	case kindUint32:
		var out []*uint32

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := uint32(vs[i].Int32())
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*uint32)(dst) = out
		}
	case kindUint64:
		var out []*uint64

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := uint64(vs[i].Int64())
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*uint64)(dst) = out
		}
	case kindFloat32:
		var out []*float32

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := vs[i].Float()
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*float32)(dst) = out
		}
	case kindFloat64:
		var out []*float64

		for i := range vs {
			if vs[i].DefinitionLevel() < elemLevel {
				continue
			}

			if vs[i].IsNull() {
				out = append(out, nil)
			} else {
				v := vs[i].Double()
				out = append(out, &v)
			}
		}

		if out != nil {
			*(*[]*float64)(dst) = out
		}
	}
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
