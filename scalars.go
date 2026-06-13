package parquetfast

import (
	"reflect"

	"github.com/parquet-go/parquet-go"
)

// Scalars: required Go fields whose value is built from exactly one parquet leaf
// column (string, bool, int*, uint*, float*, []byte).

// addScalarLeaf appends a scalar setter for the leaf column resolved from path.
func addScalarLeaf(plan *Plan, offset uintptr, path []string, schema *parquet.Schema, kind setterKind) error {
	leaf, ok := schema.Lookup(path...)
	if !ok {
		// Schema evolution: a Go field tagged for a column the file lacks
		// decodes as the zero value. Lets older parquet files still read after
		// the struct gains a new field.
		return nil
	}

	if !validColumn(leaf.ColumnIndex, plan.numLeaves) {
		return nil
	}

	// Column is proven 100% null in the file — leave the field at its zero value.
	if plan.isSkipped(leaf.ColumnIndex) {
		return nil
	}

	plan.scalars = append(plan.scalars, scalarSetter{
		offset: offset,
		col:    int32(leaf.ColumnIndex),
		kind:   kind,
	})

	return nil
}

// scalarKindFor returns the setterKind for a plain-scalar Go kind. Narrow ints
// (int8/int16/uint8/uint16) are supported and widen from the INT32 the file
// stores. reflect.Uint8 here is a bare uint8 field; []uint8 is handled as
// BYTE_ARRAY in addFieldByKind before this is consulted for the element kind.
func scalarKindFor(k reflect.Kind) (setterKind, bool) {
	switch k {
	case reflect.String:
		return kindString, true
	case reflect.Bool:
		return kindBool, true
	case reflect.Int:
		return kindInt, true
	case reflect.Int8:
		return kindInt8, true
	case reflect.Int16:
		return kindInt16, true
	case reflect.Int32:
		return kindInt32, true
	case reflect.Int64:
		return kindInt64, true
	case reflect.Uint8:
		return kindUint8, true
	case reflect.Uint16:
		return kindUint16, true
	case reflect.Uint32:
		return kindUint32, true
	case reflect.Uint64:
		return kindUint64, true
	case reflect.Float32:
		return kindFloat32, true
	case reflect.Float64:
		return kindFloat64, true
	default:
		return 0, false
	}
}
