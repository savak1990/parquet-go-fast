package parquetfast

import (
	"reflect"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Structs: optional *T struct fields. Required structs don't need a dedicated
// handler — addStructFields in plan.go recurses into them at the same offset, so
// every descendant scalar binds directly into the parent struct's memory.
// Optional structs need allocation + sub-plan application + a presence gate.

// addOptionalStruct installs a compound for a *Struct field. The handler peeks at
// every descendant leaf's definition level and, if any leaf's def is at least
// the outer's own def threshold, allocates a fresh *Struct and applies the
// sub-plan into it. Otherwise the field is cleared to nil.
func addOptionalStruct(plan *Plan, st reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	sub := plan.newSubPlan()
	if err := addStructFields(sub, st, 0, path, schema); err != nil {
		return err
	}

	// If no descendant Go field binds anything, the compound has nothing to
	// populate — skip installing it.
	if !sub.hasBindings() {
		return nil
	}

	// Presence detection scans ALL schema descendants of path (not just sub's
	// bindings), so the gate fires correctly even when the struct's only
	// descendants are themselves compounds (e.g. nested optional structs).
	descendantCols := descendantLeafCols(schema, path, plan.numLeaves)
	if len(descendantCols) == 0 {
		return nil
	}

	outerDefLevel := defLevelOfPath(schema, path)
	if outerDefLevel < 0 {
		// Path doesn't resolve — schema-evolution case; leave the field nil.
		return nil
	}

	// A present optional struct reads all its descendant columns: the sub-plan
	// binds some, and the presence gate (anyDescendantPresent) inspects them all.
	// Mark them referenced so projection never masks a presence-detection column.
	plan.markRefs(descendantCols)

	alloc, typed := typedOptionalStructAlloc(st)

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		structPtr := unsafe.Add(base, offset)

		if !anyDescendantPresent(descendantCols, leafVals, outerDefLevel) {
			// Clear the field — defensive against callers reusing the
			// destination across Apply calls.
			*(*unsafe.Pointer)(structPtr) = nil

			return
		}

		var newPtr unsafe.Pointer
		if typed {
			newPtr = alloc()
		} else {
			newPtr = reflect.New(st).UnsafePointer()
		}

		sub.applyDecoded(newPtr, leafVals)
		// Storing newPtr roots the heap object via the parent's *T field.
		*(*unsafe.Pointer)(structPtr) = newPtr
	})

	return nil
}

// anyDescendantPresent returns true if any of the given descendant columns
// carries a value with def at least outerDefLevel — i.e. the parquet writer
// recorded the outer struct as present at this row's depth.
func anyDescendantPresent(cols []int, leafVals [][]parquet.Value, outerDefLevel int) bool {
	for _, col := range cols {
		if col >= len(leafVals) {
			continue
		}

		vs := leafVals[col]
		if len(vs) == 0 {
			continue
		}

		if vs[0].DefinitionLevel() >= outerDefLevel {
			return true
		}
	}

	return false
}

// descendantLeafCols returns every leaf column index whose schema path starts
// with prefix. Cold path (plan-build time).
func descendantLeafCols(schema *parquet.Schema, prefix []string, numLeaves int) []int {
	var cols []int

	for _, p := range schema.Columns() {
		if !pathStartsWith(p, prefix) {
			continue
		}

		leaf, ok := schema.Lookup(p...)
		if !ok || !validColumn(leaf.ColumnIndex, numLeaves) {
			continue
		}

		cols = append(cols, leaf.ColumnIndex)
	}

	return cols
}
