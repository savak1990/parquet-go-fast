package parquetfast

import (
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// setterKind enumerates every scalar / optional-scalar Go field shape the hot
// path can write. It replaces the closure-per-leaf design: instead of storing a
// captured func value per column, the plan stores a compact (col, kind, offset)
// descriptor and the per-row loop dispatches through a single switch. The switch
// compiles to a jump table, so there is no func-pointer call per leaf and the
// typed write inlines — measurably faster than closure indirection on the
// high-density scalar path.
type setterKind uint8

const (
	// Required scalars: leave the Go zero value untouched on a null parquet
	// value (matches the per-type setters they replace).
	kindString setterKind = iota
	kindBool
	kindInt
	kindInt8
	kindInt16
	kindInt32
	kindInt64
	kindUint8
	kindUint16
	kindUint32
	kindUint64
	kindFloat32
	kindFloat64
	kindBytes

	// Optional scalars (*T): write a fresh pointer-to-value, or nil on a null
	// parquet value / an absent column. Every optional kind sorts at or above
	// firstOptKind so the "absent column" branch can detect optionals with a
	// single range compare.
	kindOptString
	kindOptBool
	kindOptInt
	kindOptInt8
	kindOptInt16
	kindOptInt32
	kindOptInt64
	kindOptUint8
	kindOptUint16
	kindOptUint32
	kindOptUint64
	kindOptFloat32
	kindOptFloat64
	kindOptBytes
)

// firstOptKind is the boundary between required and optional kinds. A kind
// >= firstOptKind writes a pointer field and must be cleared to nil when its
// column carries no value for the row.
const firstOptKind = kindOptString

// scalarSetter binds one parquet leaf column to one Go struct field. 16 bytes,
// no func pointer — a contiguous []scalarSetter is cache-friendly to iterate.
type scalarSetter struct {
	offset uintptr    // byte offset of the field within the destination struct
	col    int32      // parquet leaf column index
	kind   setterKind // which typed write to perform
}

// applyScalar performs the typed write for one leaf value into the field at dst.
// Narrow ints (int8/int16/uint8/uint16) widen from the INT32 the file stores.
// Required scalars no-op on a null value; the bytes/optional kinds clear to nil.
func applyScalar(kind setterKind, dst unsafe.Pointer, v parquet.Value) {
	switch kind {
	// ── required scalars ──────────────────────────────────────────────
	case kindString:
		if !v.IsNull() {
			*(*string)(dst) = string(v.ByteArray())
		}
	case kindBool:
		if !v.IsNull() {
			*(*bool)(dst) = v.Boolean()
		}
	case kindInt:
		if !v.IsNull() {
			*(*int)(dst) = int(v.Int64())
		}
	case kindInt8:
		if !v.IsNull() {
			*(*int8)(dst) = int8(v.Int32())
		}
	case kindInt16:
		if !v.IsNull() {
			*(*int16)(dst) = int16(v.Int32())
		}
	case kindInt32:
		if !v.IsNull() {
			*(*int32)(dst) = v.Int32()
		}
	case kindInt64:
		if !v.IsNull() {
			*(*int64)(dst) = v.Int64()
		}
	case kindUint8:
		if !v.IsNull() {
			*(*uint8)(dst) = uint8(v.Int32())
		}
	case kindUint16:
		if !v.IsNull() {
			*(*uint16)(dst) = uint16(v.Int32())
		}
	case kindUint32:
		if !v.IsNull() {
			*(*uint32)(dst) = uint32(v.Int32())
		}
	case kindUint64:
		if !v.IsNull() {
			*(*uint64)(dst) = uint64(v.Int64())
		}
	case kindFloat32:
		if !v.IsNull() {
			*(*float32)(dst) = v.Float()
		}
	case kindFloat64:
		if !v.IsNull() {
			*(*float64)(dst) = v.Double()
		}
	case kindBytes:
		// []byte clears to nil on null (unlike numeric scalars which no-op),
		// then copies off the reusable parquet-go page buffer.
		*(*[]byte)(dst) = copyBytes(v)

	// ── optional scalars (*T) ─────────────────────────────────────────
	case kindOptString:
		if v.IsNull() {
			*(**string)(dst) = nil
		} else {
			x := string(v.ByteArray())
			*(**string)(dst) = &x
		}
	case kindOptBool:
		if v.IsNull() {
			*(**bool)(dst) = nil
		} else {
			x := v.Boolean()
			*(**bool)(dst) = &x
		}
	case kindOptInt:
		if v.IsNull() {
			*(**int)(dst) = nil
		} else {
			x := int(v.Int64())
			*(**int)(dst) = &x
		}
	case kindOptInt8:
		if v.IsNull() {
			*(**int8)(dst) = nil
		} else {
			x := int8(v.Int32())
			*(**int8)(dst) = &x
		}
	case kindOptInt16:
		if v.IsNull() {
			*(**int16)(dst) = nil
		} else {
			x := int16(v.Int32())
			*(**int16)(dst) = &x
		}
	case kindOptInt32:
		if v.IsNull() {
			*(**int32)(dst) = nil
		} else {
			x := v.Int32()
			*(**int32)(dst) = &x
		}
	case kindOptInt64:
		if v.IsNull() {
			*(**int64)(dst) = nil
		} else {
			x := v.Int64()
			*(**int64)(dst) = &x
		}
	case kindOptUint8:
		if v.IsNull() {
			*(**uint8)(dst) = nil
		} else {
			x := uint8(v.Int32())
			*(**uint8)(dst) = &x
		}
	case kindOptUint16:
		if v.IsNull() {
			*(**uint16)(dst) = nil
		} else {
			x := uint16(v.Int32())
			*(**uint16)(dst) = &x
		}
	case kindOptUint32:
		if v.IsNull() {
			*(**uint32)(dst) = nil
		} else {
			x := uint32(v.Int32())
			*(**uint32)(dst) = &x
		}
	case kindOptUint64:
		if v.IsNull() {
			*(**uint64)(dst) = nil
		} else {
			x := uint64(v.Int64())
			*(**uint64)(dst) = &x
		}
	case kindOptFloat32:
		if v.IsNull() {
			*(**float32)(dst) = nil
		} else {
			x := v.Float()
			*(**float32)(dst) = &x
		}
	case kindOptFloat64:
		if v.IsNull() {
			*(**float64)(dst) = nil
		} else {
			x := v.Double()
			*(**float64)(dst) = &x
		}
	case kindOptBytes:
		if v.IsNull() {
			*(**[]byte)(dst) = nil
		} else {
			b := copyBytes(v)
			*(**[]byte)(dst) = &b
		}
	}
}

// copyBytes returns a fresh copy of v's byte payload (the parquet-go page buffer
// is reused across rows, so the bytes must be copied off). Returns nil for a
// null or empty value.
func copyBytes(v parquet.Value) []byte {
	if v.IsNull() {
		return nil
	}

	src := v.ByteArray()
	if src == nil {
		return nil
	}

	out := make([]byte, len(src))
	copy(out, src)

	return out
}
