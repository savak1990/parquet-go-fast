package parquetfast

import (
	"reflect"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Maps: Go fields of kind reflect.Map. Three flavours:
//
//   - Primitive maps (map[K]primitive_V) — addPrimitiveMap. Typed fast paths for
//     the common Go-only combos, reflect fallback for everything else.
//   - Struct-valued maps (map[K]Struct) — addStructValuedMap. Typed fast paths
//     registered per-(K, V) via RegisterStructValuedMap; reflect fallback for
//     unregistered combos.
//   - Nested maps (map[K1]map[K2]V) — addNestedMap.

// addMapField dispatches a map[K]V Go field to the right registrar.
func addMapField(plan *Plan, mt reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	// time.Time as a map value is a single leaf, not a struct subtree — route it
	// before the generic struct-valued-map handling.
	if mt.Elem() == timeType {
		return addTimeValuedMap(plan, mt, offset, path, schema)
	}

	switch mt.Elem().Kind() {
	case reflect.Struct:
		return addStructValuedMap(plan, mt, offset, path, schema)
	case reflect.Map:
		return addNestedMap(plan, mt, offset, path, schema)
	default:
		return addPrimitiveMap(plan, mt, offset, path, schema)
	}
}

// addPrimitiveMap installs a compound that builds map[K]V where V is primitive.
func addPrimitiveMap(plan *Plan, mt reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	keyPath := appendPath(path, pqtKeyValueSeg, pqtKeySeg)
	valPath := appendPath(path, pqtKeyValueSeg, pqtValueSeg)

	keyLeaf, okk := schema.Lookup(keyPath...)
	valLeaf, okv := schema.Lookup(valPath...)
	if !okk || !okv {
		return nil
	}

	keyCol, valCol := keyLeaf.ColumnIndex, valLeaf.ColumnIndex
	if !validColumn(keyCol, plan.numLeaves) || !validColumn(valCol, plan.numLeaves) {
		return nil
	}

	insertFn := primitiveMapInsertFn(mt)
	if insertFn == nil {
		return unsupportedKindErr(mt, path)
	}

	plan.markRef(keyCol)
	plan.markRef(valCol)

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		keys := leafVals[keyCol]
		if len(keys) == 0 {
			return
		}

		// Parquet emits a single null value for absent/empty maps.
		if len(keys) == 1 && keys[0].IsNull() {
			return
		}

		insertFn(unsafe.Add(base, offset), keys, leafVals[valCol])
	})

	return nil
}

// primitiveMapInsertFn returns a typed-fast-path inserter for the common
// key/value combos, or a reflect-based fallback for everything else.
func primitiveMapInsertFn(mt reflect.Type) func(unsafe.Pointer, []parquet.Value, []parquet.Value) {
	switch mt {
	case reflect.TypeFor[map[string]string]():
		return insertMapStringString
	case reflect.TypeFor[map[string]int64]():
		return insertMapStringInt64
	case reflect.TypeFor[map[int64]float64]():
		return insertMapInt64Float64
	}

	return reflectPrimitiveMapInsertFn(mt)
}

func insertMapStringString(mapPtr unsafe.Pointer, keys, vals []parquet.Value) {
	m := *(*map[string]string)(mapPtr)
	if m == nil {
		m = make(map[string]string, len(keys))
	}

	for i := range keys {
		if keys[i].IsNull() {
			continue
		}

		var v string
		if i < len(vals) && !vals[i].IsNull() {
			v = string(vals[i].ByteArray())
		}

		m[string(keys[i].ByteArray())] = v
	}

	*(*map[string]string)(mapPtr) = m
}

func insertMapStringInt64(mapPtr unsafe.Pointer, keys, vals []parquet.Value) {
	m := *(*map[string]int64)(mapPtr)
	if m == nil {
		m = make(map[string]int64, len(keys))
	}

	for i := range keys {
		if keys[i].IsNull() {
			continue
		}

		var v int64
		if i < len(vals) && !vals[i].IsNull() {
			v = vals[i].Int64()
		}

		m[string(keys[i].ByteArray())] = v
	}

	*(*map[string]int64)(mapPtr) = m
}

func insertMapInt64Float64(mapPtr unsafe.Pointer, keys, vals []parquet.Value) {
	m := *(*map[int64]float64)(mapPtr)
	if m == nil {
		m = make(map[int64]float64, len(keys))
	}

	for i := range keys {
		if keys[i].IsNull() {
			continue
		}

		var v float64
		if i < len(vals) && !vals[i].IsNull() {
			v = vals[i].Double()
		}

		m[keys[i].Int64()] = v
	}

	*(*map[int64]float64)(mapPtr) = m
}

func reflectPrimitiveMapInsertFn(mt reflect.Type) func(unsafe.Pointer, []parquet.Value, []parquet.Value) {
	kt, vt := mt.Key(), mt.Elem()
	keyFromValue := primitiveKeyExtractor(kt)
	valFromValue := primitiveValueExtractor(vt)

	if keyFromValue == nil || valFromValue == nil {
		return nil
	}

	return func(mapPtr unsafe.Pointer, keys, vals []parquet.Value) {
		mv := reflect.NewAt(mt, mapPtr).Elem()
		if mv.IsNil() {
			mv.Set(reflect.MakeMapWithSize(mt, len(keys)))
		}

		for i := range keys {
			if keys[i].IsNull() {
				continue
			}

			k := keyFromValue(keys[i])

			var v reflect.Value
			if i < len(vals) && !vals[i].IsNull() {
				v = valFromValue(vals[i])
			} else {
				v = reflect.Zero(vt)
			}

			mv.SetMapIndex(k.Convert(kt), v.Convert(vt))
		}
	}
}

// addStructValuedMap installs a compound for map[K]Struct. Walks the value
// subtree once at build time to classify each inner leaf as scalar or nested,
// then at decode iterates the actual key column (length-checked) and rebuilds
// per-entry leafVals for the sub-plan. Uses a registered typed filler when
// available, else a reflect path.
func addStructValuedMap(plan *Plan, mt reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	keyPath := appendPath(path, pqtKeyValueSeg, pqtKeySeg)

	keyLeaf, ok := schema.Lookup(keyPath...)
	if !ok || !validColumn(keyLeaf.ColumnIndex, plan.numLeaves) {
		return nil
	}

	st := mt.Elem()
	valPath := appendPath(path, pqtKeyValueSeg, pqtValueSeg)

	sub := plan.newSubPlan()
	if err := addStructFields(sub, st, 0, valPath, schema); err != nil {
		return err
	}

	outerRep := repLevelOfPath(schema, appendPath(path, pqtKeyValueSeg))
	if outerRep < 0 {
		return nil
	}

	ctx := structValuedMapCtx{
		mt:        mt,
		st:        st,
		sub:       sub,
		info:      collectSubtreeInfo(schema, valPath, plan.numLeaves, outerRep, plan.skipCol),
		keyCol:    keyLeaf.ColumnIndex,
		outerRep:  outerRep,
		numLeaves: plan.numLeaves,
		offset:    offset,
	}

	// A present struct-valued map reads the key column and its whole value
	// subtree (entry iteration + rep-splitting), so mark all of it referenced.
	plan.markRef(ctx.keyCol)
	plan.markRefs(ctx.info.scalarCols)
	plan.markRefs(ctx.info.nestedCols)

	if filler, ok := typedStructValuedMapFiller(mt); ok {
		plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
			keys := leafVals[ctx.keyCol]
			if len(keys) == 0 || (len(keys) == 1 && keys[0].IsNull()) {
				return
			}

			filler(unsafe.Add(base, ctx.offset), keys, ctx.sub, ctx.info, ctx.outerRep, ctx.numLeaves, leafVals)
		})

		return nil
	}

	// Reflect fallback decodes K via primitiveKeyExtractor.
	if primitiveKeyExtractor(mt.Key()) == nil {
		return unsupportedKindErr(mt, path)
	}

	return addStructValuedMapReflect(plan, ctx)
}

// structValuedMapCtx bundles the plan-build-time values shared by the typed and
// reflect-path compounds for a struct-valued map.
type structValuedMapCtx struct {
	mt        reflect.Type
	st        reflect.Type
	sub       *Plan
	info      subtreeInfo
	keyCol    int
	outerRep  int
	numLeaves int
	offset    uintptr
}

// addStructValuedMapReflect is the reflect-based fallback used when no typed
// filler is registered for ctx.mt.
func addStructValuedMapReflect(plan *Plan, ctx structValuedMapCtx) error {
	mt, st, sub := ctx.mt, ctx.st, ctx.sub
	info, keyCol, outerRep := ctx.info, ctx.keyCol, ctx.outerRep
	numLeaves, offset := ctx.numLeaves, ctx.offset
	kt := mt.Key()
	keyExtract := primitiveKeyExtractor(kt) // non-nil: addStructValuedMap gates this.
	scalarCols, nestedCols := info.scalarCols, info.nestedCols

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		keys := leafVals[keyCol]
		if len(keys) == 0 || (len(keys) == 1 && keys[0].IsNull()) {
			return
		}

		n := len(keys)
		mapPtr := unsafe.Add(base, offset)
		mv := reflect.NewAt(mt, mapPtr).Elem()
		if mv.IsNil() {
			mv.Set(reflect.MakeMapWithSize(mt, n))
		}

		nestedSplits := splitNestedLeaves(leafVals, outerRep, numLeaves, nestedCols)
		bufp := getEntryLeafValsBuf(numLeaves)
		buf := *bufp

		for i := range n {
			if keys[i].IsNull() {
				continue
			}

			resetEntryLeafVals(buf, scalarCols)
			resetEntryLeafVals(buf, nestedCols)
			fillEntryScalarLeaves(buf, leafVals, scalarCols, i)

			if nestedSplits != nil {
				fillEntryLeafVals(buf, nestedSplits, nestedCols, i)
			}

			entry := reflect.New(st).Elem()
			sub.applyDecoded(entry.Addr().UnsafePointer(), buf)
			mv.SetMapIndex(keyExtract(keys[i]).Convert(kt), entry)
		}

		putEntryLeafValsBuf(bufp)
	})

	return nil
}

// nestedMapPlan captures the precomputed columns + extractors for a
// map[K1]map[K2]V2 compound.
type nestedMapPlan struct {
	mt          reflect.Type
	innerType   reflect.Type
	innerVt     reflect.Type
	outerKt     reflect.Type
	innerKt     reflect.Type
	outerKeyEx  func(parquet.Value) reflect.Value
	innerKeyEx  func(parquet.Value) reflect.Value
	innerValEx  func(parquet.Value) reflect.Value
	outerRep    int
	outerKeyCol int
	innerKeyCol int
	innerValCol int
	offset      uintptr
}

// addNestedMap installs a compound for map[K1]map[K2]V2 with primitive inner V.
func addNestedMap(plan *Plan, mt reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
	innerType := mt.Elem()
	if primitiveKeyExtractor(mt.Key()) == nil ||
		primitiveKeyExtractor(innerType.Key()) == nil ||
		primitiveValueExtractor(innerType.Elem()) == nil {
		return unsupportedKindErr(mt, path)
	}

	np, ok := buildNestedMapPlan(plan, mt, offset, path, schema)
	if !ok {
		return nil
	}

	plan.markRef(np.outerKeyCol)
	plan.markRef(np.innerKeyCol)
	plan.markRef(np.innerValCol)
	plan.compound = append(plan.compound, np.run)

	return nil
}

func buildNestedMapPlan(plan *Plan, mt reflect.Type, offset uintptr, path []string, schema *parquet.Schema) (nestedMapPlan, bool) {
	outerKeyPath := appendPath(path, pqtKeyValueSeg, pqtKeySeg)
	innerBase := appendPath(path, pqtKeyValueSeg, pqtValueSeg)
	innerKeyPath := appendPath(innerBase, pqtKeyValueSeg, pqtKeySeg)
	innerValPath := appendPath(innerBase, pqtKeyValueSeg, pqtValueSeg)

	outerKeyLeaf, okk := schema.Lookup(outerKeyPath...)
	innerKeyLeaf, okik := schema.Lookup(innerKeyPath...)
	innerValLeaf, okiv := schema.Lookup(innerValPath...)
	if !okk || !okik || !okiv {
		return nestedMapPlan{}, false
	}

	if !validColumn(outerKeyLeaf.ColumnIndex, plan.numLeaves) ||
		!validColumn(innerKeyLeaf.ColumnIndex, plan.numLeaves) ||
		!validColumn(innerValLeaf.ColumnIndex, plan.numLeaves) {
		return nestedMapPlan{}, false
	}

	innerType := mt.Elem()

	outerRep := repLevelOfPath(schema, appendPath(path, pqtKeyValueSeg))
	if outerRep < 0 {
		return nestedMapPlan{}, false
	}

	return nestedMapPlan{
		mt:          mt,
		innerType:   innerType,
		innerVt:     innerType.Elem(),
		outerKt:     mt.Key(),
		innerKt:     innerType.Key(),
		outerKeyEx:  primitiveKeyExtractor(mt.Key()),
		innerKeyEx:  primitiveKeyExtractor(innerType.Key()),
		innerValEx:  primitiveValueExtractor(innerType.Elem()),
		outerRep:    outerRep,
		outerKeyCol: outerKeyLeaf.ColumnIndex,
		innerKeyCol: innerKeyLeaf.ColumnIndex,
		innerValCol: innerValLeaf.ColumnIndex,
		offset:      offset,
	}, true
}

func (np nestedMapPlan) run(base unsafe.Pointer, leafVals [][]parquet.Value) {
	keys := leafVals[np.outerKeyCol]
	if len(keys) == 0 || (len(keys) == 1 && keys[0].IsNull()) {
		return
	}

	n := len(keys)
	innerKeySplits := splitByRep(leafVals[np.innerKeyCol], np.outerRep)
	innerValSplits := splitByRep(leafVals[np.innerValCol], np.outerRep)

	mapPtr := unsafe.Add(base, np.offset)
	mv := reflect.NewAt(np.mt, mapPtr).Elem()
	if mv.IsNil() {
		mv.Set(reflect.MakeMapWithSize(np.mt, n))
	}

	for i := range n {
		if keys[i].IsNull() {
			continue
		}

		var iKeys, iVals []parquet.Value
		if i < len(innerKeySplits) {
			iKeys = innerKeySplits[i]
		}

		if i < len(innerValSplits) {
			iVals = innerValSplits[i]
		}

		mv.SetMapIndex(
			np.outerKeyEx(keys[i]).Convert(np.outerKt),
			np.buildInner(iKeys, iVals),
		)
	}
}

func (np nestedMapPlan) buildInner(iKeys, iVals []parquet.Value) reflect.Value {
	inner := reflect.MakeMapWithSize(np.innerType, len(iKeys))
	for j := range iKeys {
		if iKeys[j].IsNull() {
			continue
		}

		var vv reflect.Value
		if j < len(iVals) && !iVals[j].IsNull() {
			vv = np.innerValEx(iVals[j])
		} else {
			vv = reflect.Zero(np.innerVt)
		}

		inner.SetMapIndex(np.innerKeyEx(iKeys[j]).Convert(np.innerKt), vv.Convert(np.innerVt))
	}

	return inner
}

// primitiveKeyExtractor returns a reflect.Value-yielding decoder for a Go scalar
// kind usable as a parquet map key. Bool is excluded — parquet map keys aren't
// BOOLEAN.
func primitiveKeyExtractor(kt reflect.Type) func(parquet.Value) reflect.Value {
	switch kt.Kind() {
	case reflect.String:
		return func(v parquet.Value) reflect.Value { return reflect.ValueOf(string(v.ByteArray())) }
	case reflect.Int64:
		return func(v parquet.Value) reflect.Value { return reflect.ValueOf(v.Int64()) }
	case reflect.Int32:
		return func(v parquet.Value) reflect.Value { return reflect.ValueOf(v.Int32()) }
	case reflect.Float64:
		return func(v parquet.Value) reflect.Value { return reflect.ValueOf(v.Double()) }
	}

	return nil
}

// primitiveValueExtractor is primitiveKeyExtractor plus the Bool arm (bool is
// valid as a map value but not as a key).
func primitiveValueExtractor(vt reflect.Type) func(parquet.Value) reflect.Value {
	if vt.Kind() == reflect.Bool {
		return func(v parquet.Value) reflect.Value { return reflect.ValueOf(v.Boolean()) }
	}

	return primitiveKeyExtractor(vt)
}
