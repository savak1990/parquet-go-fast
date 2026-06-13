package parquetfast

import (
	"reflect"

	"github.com/parquet-go/parquet-go"
)

// Optional primitives: *T Go fields where T is a scalar (string, bool, int*,
// uint*, float*, []byte). 1-leaf-1-field shape like scalars.go, but the setter
// writes a pointer-to-value (or nil on null / absent column).

// addOptionalPrimitive appends a setter that writes a *T field at offset.
func addOptionalPrimitive(plan *Plan, offset uintptr, path []string, schema *parquet.Schema, kind setterKind) error {
	leaf, ok := schema.Lookup(path...)
	if !ok {
		return nil
	}

	if !validColumn(leaf.ColumnIndex, plan.numLeaves) {
		return nil
	}

	// 100%-null column: skip the setter; the field's *T stays nil.
	if plan.isSkipped(leaf.ColumnIndex) {
		return nil
	}

	plan.markRef(leaf.ColumnIndex)
	plan.scalars = append(plan.scalars, scalarSetter{
		offset: offset,
		col:    int32(leaf.ColumnIndex),
		kind:   kind,
	})

	return nil
}

// optionalKindFor returns the optional setterKind for a *T pointer's element
// type T. *[]byte (optional BYTE_ARRAY) is supported in addition to the scalar
// element types.
func optionalKindFor(elem reflect.Type) (setterKind, bool) {
	if elem.Kind() == reflect.Slice && elem.Elem().Kind() == reflect.Uint8 {
		return kindOptBytes, true
	}

	switch elem.Kind() {
	case reflect.String:
		return kindOptString, true
	case reflect.Bool:
		return kindOptBool, true
	case reflect.Int:
		return kindOptInt, true
	case reflect.Int8:
		return kindOptInt8, true
	case reflect.Int16:
		return kindOptInt16, true
	case reflect.Int32:
		return kindOptInt32, true
	case reflect.Int64:
		return kindOptInt64, true
	case reflect.Uint8:
		return kindOptUint8, true
	case reflect.Uint16:
		return kindOptUint16, true
	case reflect.Uint32:
		return kindOptUint32, true
	case reflect.Uint64:
		return kindOptUint64, true
	case reflect.Float32:
		return kindOptFloat32, true
	case reflect.Float64:
		return kindOptFloat64, true
	}

	return 0, false
}
