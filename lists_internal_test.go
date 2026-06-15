package parquetfast

import (
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// White-box coverage for appendPrimitivePtrSlice across every element kind.
// Integration correctness (true null elements from a real producer) is covered
// by the conformance suite's list_columns case; here we exercise each typed arm
// directly, since parquet-go's own writer cannot faithfully emit []*T fixtures.
//
// Values are crafted with definition levels relative to elemLevel=1: a present
// element at def 2, a null element at def 1 (a real but null slot → nil), and a
// below-level placeholder at def 0 (a null/empty list → no slot).

func TestAppendPrimitivePtrSlice_AllKinds(t *testing.T) {
	const col = 0

	present := func(v parquet.Value) parquet.Value { return v.Level(0, 2, col) }
	nullElem := parquet.NullValue().Level(0, 1, col)

	t.Run("int64", func(t *testing.T) {
		var out []*int64

		appendPrimitivePtrSlice(kindInt64, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int64Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("int64: %v", derefAny(out))
		}
	})

	t.Run("int32", func(t *testing.T) {
		var out []*int32

		appendPrimitivePtrSlice(kindInt32, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int32Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("int32: %v", derefAny(out))
		}
	})

	t.Run("int", func(t *testing.T) {
		var out []*int

		appendPrimitivePtrSlice(kindInt, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int64Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("int: %v", derefAny(out))
		}
	})

	t.Run("int8", func(t *testing.T) {
		var out []*int8

		appendPrimitivePtrSlice(kindInt8, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int32Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("int8: %v", derefAny(out))
		}
	})

	t.Run("int16", func(t *testing.T) {
		var out []*int16

		appendPrimitivePtrSlice(kindInt16, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int32Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("int16: %v", derefAny(out))
		}
	})

	t.Run("uint16", func(t *testing.T) {
		var out []*uint16

		appendPrimitivePtrSlice(kindUint16, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int32Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("uint16: %v", derefAny(out))
		}
	})

	t.Run("uint32", func(t *testing.T) {
		var out []*uint32

		appendPrimitivePtrSlice(kindUint32, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int32Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("uint32: %v", derefAny(out))
		}
	})

	t.Run("uint64", func(t *testing.T) {
		var out []*uint64

		appendPrimitivePtrSlice(kindUint64, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.Int64Value(7)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 7 || out[1] != nil {
			t.Fatalf("uint64: %v", derefAny(out))
		}
	})

	t.Run("float32", func(t *testing.T) {
		var out []*float32

		appendPrimitivePtrSlice(kindFloat32, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.FloatValue(1.5)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 1.5 || out[1] != nil {
			t.Fatalf("float32: %v", derefAny(out))
		}
	})

	t.Run("float64", func(t *testing.T) {
		var out []*float64

		appendPrimitivePtrSlice(kindFloat64, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.DoubleValue(1.5)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != 1.5 || out[1] != nil {
			t.Fatalf("float64: %v", derefAny(out))
		}
	})

	t.Run("bool", func(t *testing.T) {
		var out []*bool

		appendPrimitivePtrSlice(kindBool, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.BooleanValue(true)), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != true || out[1] != nil {
			t.Fatalf("bool: %v", derefAny(out))
		}
	})

	t.Run("string", func(t *testing.T) {
		var out []*string

		appendPrimitivePtrSlice(kindString, unsafe.Pointer(&out),
			[]parquet.Value{present(parquet.ByteArrayValue([]byte("x"))), nullElem}, 1)

		if len(out) != 2 || out[0] == nil || *out[0] != "x" || out[1] != nil {
			t.Fatalf("string: %v", derefAny(out))
		}
	})

	// A list with only below-level placeholders (null/empty list) leaves nil.
	t.Run("null_list_stays_nil", func(t *testing.T) {
		var out []*int64

		appendPrimitivePtrSlice(kindInt64, unsafe.Pointer(&out),
			[]parquet.Value{parquet.NullValue().Level(0, 0, col)}, 1)

		if out != nil {
			t.Fatalf("expected nil slice for an empty/null list, got %v", out)
		}
	})
}

func derefAny[T any](s []*T) []any {
	out := make([]any, len(s))
	for i, p := range s {
		if p == nil {
			out[i] = "nil"
		} else {
			out[i] = *p
		}
	}

	return out
}
