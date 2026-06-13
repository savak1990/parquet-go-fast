package parquetfast

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Typed fast-path registration.
//
// All three registries are OPTIONAL performance knobs. Unregistered types decode
// correctly via a reflect-based fallback; registering a type swaps in a
// generic-typed code path that avoids reflect.New / reflect.MakeSlice /
// reflect.MakeMapWithSize / reflect.Value.SetMapIndex on the per-row hot path,
// saving an allocation (and the reflect dispatch) per entry.
//
// Call them once from an init() in the package that owns the struct types, e.g.:
//
//	func init() {
//	    parquetfast.RegisterStructAlloc[Address]()
//	    parquetfast.RegisterStructList[LineItem]()
//	    parquetfast.RegisterStructValuedMap[string, Container](func(v parquet.Value) string {
//	        return string(v.ByteArray())
//	    })
//	}

// ── optional *Struct allocators ──────────────────────────────────────────────

// typedOptionalStructAllocs maps a struct type T to a typed new(T) allocator.
var typedOptionalStructAllocs sync.Map // reflect.Type → func() unsafe.Pointer

// RegisterStructAlloc registers a typed new(T) allocator so the optional-struct
// compound for *T avoids reflect.New per row. Unregistered types fall back to
// reflect.New transparently.
func RegisterStructAlloc[T any]() {
	typedOptionalStructAllocs.Store(reflect.TypeFor[T](), func() unsafe.Pointer { return unsafe.Pointer(new(T)) })
}

// IsStructAllocRegistered reports whether a typed allocator has been registered
// for st via RegisterStructAlloc. Useful for an enforcement test that walks a
// type and asserts every reachable *Struct field has a fast-path allocator.
func IsStructAllocRegistered(st reflect.Type) bool {
	_, ok := typedOptionalStructAllocs.Load(st)

	return ok
}

func typedOptionalStructAlloc(st reflect.Type) (func() unsafe.Pointer, bool) {
	v, ok := typedOptionalStructAllocs.Load(st)
	if !ok {
		return nil, false
	}

	fn, ok := v.(func() unsafe.Pointer)

	return fn, ok
}

// ── typed []Struct fillers ───────────────────────────────────────────────────

// structListFiller builds a []T slice value at slicePtr from n entries, running
// sub against each entry's destination. Registered per concrete []T.
type structListFiller func(slicePtr unsafe.Pointer, n int, sub *Plan, ctx subtreeIterCtx, leafVals [][]parquet.Value)

var typedStructLists sync.Map // reflect.Type ([]T) → structListFiller

// RegisterStructList registers a typed fast path for []T (T a struct). Replaces
// reflect.MakeSlice + reflect.Value.Index per entry with make([]T, n) + a typed
// &s[i]. Unregistered element types fall back to the reflect path.
func RegisterStructList[T any]() {
	typedStructLists.Store(reflect.TypeFor[[]T](), structListFiller(
		func(slicePtr unsafe.Pointer, n int, sub *Plan, ctx subtreeIterCtx, leafVals [][]parquet.Value) {
			s := make([]T, n)
			applyPerEntrySubPlan(leafVals, ctx, n, func(buf [][]parquet.Value, i int) {
				sub.applyDecoded(unsafe.Pointer(&s[i]), buf)
			})
			*(*[]T)(slicePtr) = s
		},
	))
}

func typedStructListFiller(st reflect.Type) (structListFiller, bool) {
	v, ok := typedStructLists.Load(st)
	if !ok {
		return nil, false
	}

	fn, ok := v.(structListFiller)

	return fn, ok
}

// ── typed map[K]Struct fillers ───────────────────────────────────────────────

// structValuedMapFiller is the typed-fast-path callback registered via
// RegisterStructValuedMap. One filler per concrete map[K]V combo; avoids
// reflect.SetMapIndex / reflect.New / reflect.MakeMapWithSize on the hot path.
type structValuedMapFiller func(
	mapPtr unsafe.Pointer,
	keys []parquet.Value,
	sub *Plan,
	info subtreeInfo,
	outerRep, numLeaves int,
	leafVals [][]parquet.Value,
)

var typedStructValuedMaps sync.Map // reflect.Type → structValuedMapFiller

// RegisterStructValuedMap registers a typed fast path for map[K]V where V is a
// struct. The keyDecode callback decodes a parquet.Value into a K. Unregistered
// (K, V) combos fall back to a reflect path (which supports key kinds
// string/int32/int64/float64).
func RegisterStructValuedMap[K comparable, V any](keyDecode func(parquet.Value) K) {
	typedStructValuedMaps.Store(reflect.TypeFor[map[K]V](), structValuedMapFiller(
		func(mapPtr unsafe.Pointer, keys []parquet.Value, sub *Plan, info subtreeInfo, outerRep, numLeaves int, leafVals [][]parquet.Value) {
			m := *(*map[K]V)(mapPtr)
			if m == nil {
				m = make(map[K]V, len(keys))
			}

			ctx := subtreeIterCtx{info: info, outerRep: outerRep, numLeaves: numLeaves}
			applyPerEntrySubPlan(leafVals, ctx, len(keys), func(buf [][]parquet.Value, i int) {
				if keys[i].IsNull() {
					return
				}

				var entry V
				sub.applyDecoded(unsafe.Pointer(&entry), buf)
				m[keyDecode(keys[i])] = entry
			})

			*(*map[K]V)(mapPtr) = m
		},
	))
}

func typedStructValuedMapFiller(mt reflect.Type) (structValuedMapFiller, bool) {
	v, ok := typedStructValuedMaps.Load(mt)
	if !ok {
		return nil, false
	}

	fn, ok := v.(structValuedMapFiller)

	return fn, ok
}
