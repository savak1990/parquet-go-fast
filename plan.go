package parquetfast

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// A Plan compiles a Go struct + parquet schema into a flat set of leaf-column
// setters plus compound handlers, ready to decode parquet.Row buffers without
// reflection on the hot path.
//
// Two operations:
//
//	Compile(rt, schema, skip) — build (or fetch from cache) a Plan for a
//	                            (Go type, parquet schema) pair. Cold path; uses
//	                            reflection once.
//	Plan.Apply                — decode one parquet.Row into a destination struct.
//	                            Hot path; no reflection, writes via unsafe.Pointer.
type Plan struct {
	numLeaves int

	// scalars holds one descriptor per bound scalar / optional-scalar leaf,
	// dense (only columns with a Go field appear). Iterated in the per-row hot
	// loop and dispatched through the applyScalar switch.
	scalars []scalarSetter

	// compound handles Go fields whose value spans multiple leaf columns:
	// maps, lists, optional structs. Kept as closures — low call-density, so
	// the indirection cost is negligible and a tagged union would buy nothing.
	compound []compoundFn

	// skipCol, when non-nil, marks leaf columns proven 100% null in the file
	// the plan was built for. Such columns get no scalar descriptor and are
	// dropped from compound subtree info, so their read pipeline is skipped.
	// Cached plans index on a hash of this bitmap (see planKey) so files with
	// the same (rt, schema) but different null shapes don't alias.
	skipCol []bool
}

// compoundFn handles a Go field that reads from multiple leaf columns. Called
// once per row, after all scalar setters have run.
type compoundFn func(base unsafe.Pointer, leafVals [][]parquet.Value)

// NumLeaves returns the number of parquet leaf columns this plan binds against.
// Callers use it to size the per-row scratch buffer passed to Apply.
func (p *Plan) NumLeaves() int {
	return p.numLeaves
}

// isSkipped reports whether column col was proven 100% null in the file the
// plan was built for.
func (p *Plan) isSkipped(col int) bool {
	return col < len(p.skipCol) && p.skipCol[col]
}

// newSubPlan returns an empty Plan sharing this plan's leaf count and skip
// bitmap, for binding a nested struct subtree (optional struct, struct list,
// struct-valued map).
func (p *Plan) newSubPlan() *Plan {
	return &Plan{numLeaves: p.numLeaves, skipCol: p.skipCol}
}

// hasBindings reports whether any Go field bound to this (sub-)plan.
func (p *Plan) hasBindings() bool {
	return len(p.scalars) > 0 || len(p.compound) > 0
}

// Apply decodes one parquet.Row into the destination struct at base.
//
//	base     — pointer to the destination Go struct for this row. Setters write
//	           through base + captured field offset, so the struct must already
//	           be allocated (e.g. &out[i] in a pre-sized slice).
//	row      — a flat sequence of parquet.Value for one logical row.
//	leafVals — caller-owned scratch buffer of length NumLeaves(). Reused across
//	           rows; contents are overwritten on each call.
func (p *Plan) Apply(base unsafe.Pointer, row parquet.Row, leafVals [][]parquet.Value) {
	unflattenRow(row, leafVals)
	p.applyDecoded(base, leafVals)
}

// applyDecoded runs the plan's scalar setters + compounds against an
// already-unflattened leafVals. Shared by Apply (after unflattenRow) and by
// compound handlers running an inner Plan against a sub-section of the row.
func (p *Plan) applyDecoded(base unsafe.Pointer, leafVals [][]parquet.Value) {
	n := len(leafVals)

	for i := range p.scalars {
		s := &p.scalars[i]

		col := int(s.col)
		if col >= n {
			continue
		}

		vs := leafVals[col]
		if len(vs) == 0 {
			// Optionals clear to nil when the column is absent this row
			// (defensive against callers reusing the destination). Required
			// scalars leave the Go zero value.
			if s.kind >= firstOptKind {
				*(*unsafe.Pointer)(unsafe.Add(base, s.offset)) = nil
			}

			continue
		}

		applyScalar(s.kind, unsafe.Add(base, s.offset), vs[0])
	}

	for _, fn := range p.compound {
		fn(base, leafVals)
	}
}

// unflattenRow buckets a flat parquet.Row into per-column slices indexed by
// columnIndex.
//
//	row      — a flat sequence of parquet.Value for one logical row. Each Value
//	           knows its leaf column index via Value.Column().
//	leafVals — caller-owned output buffer, length == number of leaf columns.
//	           Reset and refilled on each call (no allocation): leafVals[i]
//	           points to the sub-slice of row holding column i's values, or nil.
//
// Invariant: values for the same column must be contiguous in the row
// (parquet-go's canonical row layout).
func unflattenRow(row parquet.Row, leafVals [][]parquet.Value) {
	for j := range leafVals {
		leafVals[j] = nil
	}

	rn := len(row)
	j := 0

	for j < rn {
		col := row[j].Column()
		k := j + 1

		for k < rn && row[k].Column() == col {
			k++
		}

		if col >= 0 && col < len(leafVals) {
			leafVals[col] = row[j:k:k]
		}

		j = k
	}
}

// Compile returns the compiled plan for (rt, schema, skip), building it on first
// call and serving cached copies thereafter.
//
//	rt     — the Go struct type rows will be decoded into.
//	schema — the parquet schema of the file being read; supplies the leaf
//	         column layout the plan binds field offsets to.
//	skip   — optional bitmap (len == leaf count) marking 100%-null columns; nil
//	         disables the optimization.
//
// Returns an error if any field has an unsupported kind. The cache is not
// populated on error, so the next call retries the build.
func Compile(rt reflect.Type, schema *parquet.Schema, skip []bool) (*Plan, error) {
	key := planKey{rt: rt, hash: schemaHash(schema), skipHash: skipHash(skip)}

	if p, ok := rawPlanCache.get(key); ok {
		return p, nil
	}

	plan, err := buildPlan(rt, schema, skip)
	if err != nil {
		return nil, err
	}

	return rawPlanCache.put(key, plan), nil
}

// buildPlan walks the reflect type and binds parquet paths to leaf column
// indexes. Fields with no resolving path are silently skipped (schema
// evolution). skip, when non-empty, marks 100%-null columns whose
// setters/compounds the plan omits.
func buildPlan(rt reflect.Type, schema *parquet.Schema, skip []bool) (*Plan, error) {
	plan := &Plan{
		numLeaves: len(schema.Columns()),
		skipCol:   skip,
	}

	if err := addStructFields(plan, rt, 0, nil, schema); err != nil {
		return nil, err
	}

	return plan, nil
}

// addStructFields recurses into rt, registering setters relative to baseOffset.
func addStructFields(plan *Plan, rt reflect.Type, baseOffset uintptr, pathPrefix []string, schema *parquet.Schema) error {
	if rt.Kind() != reflect.Struct {
		return fmt.Errorf("addStructFields: expected struct, got %s", rt.Kind())
	}

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}

		name := parquetTagName(f)
		if name == "" || name == "-" {
			continue
		}

		path := append(append([]string{}, pathPrefix...), name)
		offset := baseOffset + f.Offset

		if err := addFieldByKind(plan, f.Type, offset, path, schema); err != nil {
			return fmt.Errorf("%s: %w", strings.Join(path, "."), err)
		}
	}

	return nil
}

// addFieldByKind dispatches a Go field type to the right plan-builder.
func addFieldByKind(plan *Plan, ft reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	if kind, ok := scalarKindFor(ft.Kind()); ok {
		return addScalarLeaf(plan, offset, path, schema, kind)
	}

	switch ft.Kind() {
	case reflect.Slice:
		elem := ft.Elem()
		// NOTE: []byte/[]uint8 is BYTE_ARRAY, NOT a list — it must be checked
		// before the primitive-element dispatch, because a bare uint8 field is
		// a narrow int but a []uint8 field is a byte string.
		if elem.Kind() == reflect.Uint8 {
			return addScalarLeaf(plan, offset, path, schema, kindBytes)
		}

		if elem.Kind() == reflect.Struct {
			return addStructList(plan, ft, offset, path, schema)
		}

		if _, ok := scalarKindFor(elem.Kind()); ok {
			return addPrimitiveList(plan, ft, offset, path, schema)
		}

		return unsupportedKindErr(ft, path)
	case reflect.Map:
		return addMapField(plan, ft, offset, path, schema)
	case reflect.Ptr:
		elem := ft.Elem()
		if elem.Kind() == reflect.Struct {
			return addOptionalStruct(plan, elem, offset, path, schema)
		}

		if kind, ok := optionalKindFor(elem); ok {
			return addOptionalPrimitive(plan, offset, path, schema, kind)
		}

		return unsupportedKindErr(ft, path)
	case reflect.Struct:
		return addStructFields(plan, ft, offset, path, schema)
	default:
		return unsupportedKindErr(ft, path)
	}
}

func unsupportedKindErr(ft reflect.Type, path []string) error {
	return fmt.Errorf("parquet-go-fast: field kind %v at %s is not supported", ft, strings.Join(path, "."))
}

func parquetTagName(f reflect.StructField) string {
	tag := f.Tag.Get("parquet")
	if tag == "" {
		return ""
	}

	if comma := strings.IndexByte(tag, ','); comma >= 0 {
		return tag[:comma]
	}

	return tag
}

func validColumn(col, n int) bool {
	return col >= 0 && col < n
}
