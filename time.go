package parquetfast

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// time.Time support. parquet-go writes time.Time as a single leaf column whose
// logical type encodes the unit (TIMESTAMP millis/micros/nanos, or DATE). The
// stored value is an absolute Unix instant, so we reconstruct in UTC with a
// single time.Unix(...) call on the hot path — no reflection, no zone lookup.
//
// time.Time is a struct, so it must be intercepted in addFieldByKind before the
// generic struct/optional-struct handling (which would recurse into its
// unexported fields and silently decode nothing).

var timeType = reflect.TypeFor[time.Time]()

// addTimeLeaf binds a time.Time (optional=false) or *time.Time (optional=true)
// field to its leaf column, choosing the unit-specific setter kind from the
// column's logical type.
func addTimeLeaf(plan *Plan, offset uintptr, path []string, schema *parquet.Schema, optional bool) error {
	leaf, ok := schema.Lookup(path...)
	if !ok {
		return nil
	}

	if !validColumn(leaf.ColumnIndex, plan.numLeaves) {
		return nil
	}

	if plan.isSkipped(leaf.ColumnIndex) {
		return nil
	}

	kind, ok := timeKindFor(leaf.Node.Type(), optional)
	if !ok {
		return unsupportedTimeErr(path)
	}

	plan.scalars = append(plan.scalars, scalarSetter{
		offset: offset,
		col:    int32(leaf.ColumnIndex),
		kind:   kind,
	})

	return nil
}

// timeKindFor maps a column's logical/physical type to the time setter kind.
// Covers the encodings parquet-go produces for time.Time: TIMESTAMP (millis/
// micros/nanos) and DATE, plus the bare INT64 (nanos) / INT32 (days) physical
// fallbacks. Float/double/byte-array timestamp encodings are reported
// unsupported (a loud Compile error) rather than silently mis-decoded.
func timeKindFor(t parquet.Type, optional bool) (setterKind, bool) {
	if lt := t.LogicalType(); lt != nil {
		switch {
		case lt.Timestamp != nil:
			unit := lt.Timestamp.Unit
			switch {
			case unit.Millis != nil:
				return pickTimeKind(optional, kindTimeMillis, kindOptTimeMillis), true
			case unit.Micros != nil:
				return pickTimeKind(optional, kindTimeMicros, kindOptTimeMicros), true
			default:
				return pickTimeKind(optional, kindTimeNanos, kindOptTimeNanos), true
			}
		case lt.Date != nil:
			return pickTimeKind(optional, kindTimeDate, kindOptTimeDate), true
		}
	}

	switch t.Kind() {
	case parquet.Int64:
		return pickTimeKind(optional, kindTimeNanos, kindOptTimeNanos), true
	case parquet.Int32:
		return pickTimeKind(optional, kindTimeDate, kindOptTimeDate), true
	}

	return 0, false
}

func pickTimeKind(optional bool, required, opt setterKind) setterKind {
	if optional {
		return opt
	}

	return required
}

// addTimeList installs a compound for a []time.Time field (both the default
// `repeated` and the `,list` layouts).
func addTimeList(plan *Plan, offset uintptr, path []string, schema *parquet.Schema) error {
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

	kind, ok := timeKindFor(leaf.Node.Type(), false)
	if !ok {
		return unsupportedTimeErr(path)
	}

	col := leaf.ColumnIndex

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		vs := leafVals[col]
		if len(vs) == 0 {
			return
		}

		if len(vs) == 1 && vs[0].IsNull() {
			return
		}

		out := make([]time.Time, 0, len(vs))
		for i := range vs {
			if vs[i].IsNull() {
				continue
			}

			out = append(out, decodeTimeValue(kind, vs[i]))
		}

		*(*[]time.Time)(unsafe.Add(base, offset)) = out
	})

	return nil
}

// addTimeValuedMap installs a compound for map[K]time.Time. The time value is a
// single leaf (like a primitive map value), so this mirrors addPrimitiveMap with
// a time decode. Map construction uses reflect (low call density).
func addTimeValuedMap(plan *Plan, mt reflect.Type, offset uintptr, path []string, schema *parquet.Schema) error {
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

	keyEx := primitiveKeyExtractor(mt.Key())
	if keyEx == nil {
		return unsupportedKindErr(mt, path)
	}

	kind, ok := timeKindFor(valLeaf.Node.Type(), false)
	if !ok {
		return unsupportedTimeErr(path)
	}

	kt := mt.Key()

	plan.compound = append(plan.compound, func(base unsafe.Pointer, leafVals [][]parquet.Value) {
		keys := leafVals[keyCol]
		if len(keys) == 0 || (len(keys) == 1 && keys[0].IsNull()) {
			return
		}

		vals := leafVals[valCol]
		mv := reflect.NewAt(mt, unsafe.Add(base, offset)).Elem()
		if mv.IsNil() {
			mv.Set(reflect.MakeMapWithSize(mt, len(keys)))
		}

		for i := range keys {
			if keys[i].IsNull() {
				continue
			}

			var tv time.Time
			if i < len(vals) && !vals[i].IsNull() {
				tv = decodeTimeValue(kind, vals[i])
			}

			mv.SetMapIndex(keyEx(keys[i]).Convert(kt), reflect.ValueOf(tv))
		}
	})

	return nil
}

func unsupportedTimeErr(path []string) error {
	return fmt.Errorf("parquet-go-fast: time.Time at %s uses an unsupported physical encoding "+
		"(only TIMESTAMP and DATE columns are supported)", strings.Join(path, "."))
}
