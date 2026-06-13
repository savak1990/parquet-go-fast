# parquet-go-fast

A high-performance, reflection-free-on-the-hot-path **parquet decoder** for Go.

`parquet-go-fast` reads parquet files into Go structs much faster, and with far
fewer allocations, than the reflection-driven reader in
[`parquet-go/parquet-go`](https://github.com/parquet-go/parquet-go) — by doing
the schema/reflection work **once** per `(Go type, file schema)` and then
decoding every row through precompiled, typed `unsafe.Pointer` writes.

It is a decode-only library that sits **on top of** `parquet-go` (its only
dependency) and reads files written by any spec-conformant writer: `parquet-go`,
Apache Arrow, Spark, DuckDB, pandas/pyarrow, etc.

```go
import parquetfast "github.com/savak1990/parquet-go-fast"

rows, err := parquetfast.UnmarshalBytes[MyRow](data)
```

---

## Why

Profiling `parquet-go`'s `GenericReader` shows most read-path allocations come
from machinery that is avoidable when you decode into a fixed Go type:

| Cost | Share of read-path allocations |
|---|---|
| Schema `Convert` pipeline (remaps every value's levels) | ~26% |
| `rowGroupRows.ReadRows` internal buffers | ~25% |
| Reflection `Reconstruct` walkers (map / optional / leaf) | ~11% + ~6% |
| `byteArrayType.AssignValue` reflect dispatch | ~4% |

`parquet-go-fast` skips the `Convert` pipeline entirely (it reads the file's own
schema, no target-schema conversion), replaces the per-row reflection walk with
a compiled plan, and replaces per-leaf reflect dispatch with a typed
enum-switch. The reflection happens once, at plan-build time, and is cached.

---

## Quickstart

Use the same `parquet:"..."` struct tags `parquet-go`'s writer uses:

```go
type Row struct {
    Name   string            `parquet:"name"`
    Count  int64             `parquet:"count"`
    Active bool              `parquet:"active"`
    Tags   []string          `parquet:"tags"`
    Labels map[string]string `parquet:"labels"`
}

// One-shot: decode the whole file into a slice.
rows, err := parquetfast.UnmarshalBytes[Row](data)

// Or from any io.ReaderAt (e.g. *os.File):
f, _ := os.Open("data.parquet")
fi, _ := f.Stat()
rows, err := parquetfast.Unmarshal[Row](f, fi.Size())
```

For large files, stream in caller-sized batches with a reused destination
instead of materializing every row at once:

```go
rd, err := parquetfast.NewReader[Row](f, size)
if err != nil { /* ... */ }
defer rd.Close()

buf := make([]Row, 4096)
for {
    n, err := rd.Read(buf)
    process(buf[:n])
    if err == io.EOF { break }
    if err != nil { return err }
}
```

---

## How it works

Two stages: **Plan** (once) then **Apply** (per row).

### Stage 1 — Plan (built once, cached)

`Compile(reflect.Type, *parquet.Schema, skip)` walks the Go struct once and, for
every leaf field, records a compact descriptor: the **parquet leaf-column index**
it binds to, the field's **byte offset** within the struct, and a **kind enum**
selecting the typed write. Fields spanning multiple columns (maps, lists,
optional structs) get a closure. The result is cached on `(Go type, schema hash,
null-column hash)`, so the reflection cost is paid once per shape.

### Stage 2 — Apply (per row, no reflection)

```
parquet.Row (flat []parquet.Value)
        │  unflatten by Value.Column()  → leafVals[col]
        ▼
for each scalar descriptor:  switch kind { *(*T)(base+offset) = v.Xxx() }
for each compound closure:   build map / slice / *struct at base+offset
```

`base` is a pointer to the destination struct (`&out[i]`). Each scalar write is
one offset-add plus one typed store — no interface dispatch, no `reflect.Value`.

### Why an enum switch instead of closures

An earlier design stored a captured closure per leaf column. Storing a
`(col, kind, offset)` descriptor and dispatching through a single `switch` is
measurably faster: the switch compiles to a jump table, there is no
func-pointer call per leaf, and the descriptor slice is contiguous and
cache-friendly. See `BenchmarkPlanApply`.

---

## Supported types

`typed inline` = direct typed write, no reflect on decode ·
`typed fast path` = typed when registered (see [Registration](#typed-fast-path-registration)),
reflect fallback otherwise · `reflect` = reflect on the hot path ·
`unsupported` = errors at `Compile`.

| Go field type | Hot path | Notes |
|---|---|---|
| **Required scalars** | | |
| `string` | typed inline | BYTE_ARRAY → string copy |
| `bool` | typed inline | |
| `int, int8, int16, int32, int64` | typed inline | narrow ints widen from INT32 |
| `uint8, uint16, uint32, uint64` | typed inline | widen from INT32/INT64 |
| `float32, float64` | typed inline | |
| `[]byte` | typed inline | BYTE_ARRAY → byte copy |
| **Optional scalars (`*T`)** | | |
| `*string, *bool, *int…, *uint…, *float…` | typed inline | nil on null/absent |
| `*[]byte` | typed inline | optional BYTE_ARRAY |
| **Structs** | | |
| `struct{…}` | typed inline | embedded at parent offset |
| `*struct{…}` | typed fast path | `RegisterStructAlloc[T]`, else `reflect.New` |
| **Primitive slices** | | |
| `[]string, []bool, []int…, []uint16/32/64, []float…` | typed inline | both `repeated` and `,list` layouts |
| **Struct slices** | | |
| `[]Struct` | typed fast path | `RegisterStructList[T]`, else `reflect.MakeSlice` |
| **Maps — `map[K]primitive`** | | |
| `map[string]string`, `map[string]int64`, `map[int64]float64` | typed inline | |
| other `map[K]V` primitive | reflect | K ∈ {string,int32,int64,float64}; V primitive incl. bool |
| **Maps — `map[K]Struct`** | typed fast path | `RegisterStructValuedMap[K,V]`, else reflect |
| **Nested maps — `map[K1]map[K2]V`** | reflect | inner V primitive |

### Not supported (errors at `Compile`)

| Go field type | Why |
|---|---|
| `[]*T` | pointer-element slices — use `[]T` |
| `map[K][]V`, `[]map[K]V`, `map[K][]Struct` | mixed nesting: two repetition levels with no struct dispatch boundary |
| `map[K1]map[K2]Struct` | nested struct-valued maps |
| `complex`, `chan`, `func`, `interface`, arrays | not representable in parquet |

---

## Typed fast-path registration

All three registries are **optional** performance knobs. Unregistered types
decode correctly via a reflect fallback; registering swaps in a generic-typed
path that avoids `reflect.New` / `reflect.MakeSlice` / `reflect.MakeMapWithSize`
/ `SetMapIndex` per entry. Call them once from an `init()`:

```go
func init() {
    parquetfast.RegisterStructAlloc[Address]()                 // *Address fields
    parquetfast.RegisterStructList[LineItem]()                 // []LineItem fields
    parquetfast.RegisterStructValuedMap[string, Container](    // map[string]Container fields
        func(v parquet.Value) string { return string(v.ByteArray()) },
    )
}
```

---

## Benchmarks

200,000 rows of a realistically nested record (scalars, an optional struct, a
`map[string]string`, and a `map[string]Struct`), snappy-compressed, 4 row
groups. Apple M-series, Go 1.26, parquet-go v0.30.1. Synthetic data — no
external fixtures.

| Decoder | time/op | B/op | allocs/op |
|---|---:|---:|---:|
| `parquet-go` `GenericReader` (streaming) | 198 ms | 155 MB | 5.85 M |
| **`parquet-go-fast` `Reader`** (streaming) | **100 ms** | **80 MB** | **3.45 M** |
| `parquet-go-fast` `UnmarshalBytes` (whole file in memory) | 139 ms | 260 MB¹ | 4.20 M |

Streaming vs streaming (the apples-to-apples comparison): **−50% time, −48%
bytes, −41% allocations**.

¹ `UnmarshalBytes` materializes all 200k decoded rows (and their nested maps)
at once, so its byte count reflects the full result set, not steady-state
working memory. Use `Reader` when you don't need every row resident.

Reproduce:

```sh
go test -run='^$' -bench='BenchmarkDecode|BenchmarkPlanApply' -benchmem
```

---

## Limitations & safety

- **`unsafe.Pointer`.** The plan stores byte offsets and assumes the destination
  struct layout is stable between plan-build and decode. A plan is bound to a
  specific Go type + schema (the cache key encodes both); only apply it to
  instances of that type. `Unmarshal`/`Reader` manage this for you.
- **Destination must not grow during decode.** `Unmarshal` pre-allocates
  `make([]T, NumRows)` and writes through `&out[i]`; never append to a slice a
  plan is writing into.
- **`nil` vs empty.** Parquet does not distinguish a `nil` slice/map from an
  empty one for required fields; an unset required `[]byte`/`[]T`/`map` may
  decode as an empty (non-nil) value. Use optional (`*T`, or
  `,optional`-tagged) fields where the distinction matters.
- **Multi-row-group files** are supported (each group is masked for all-null
  columns, then combined via `parquet.MultiRowGroup`).
- **Concurrency.** A compiled `Plan` is read-only and safe to share. A `Reader`
  is not safe for concurrent use; create one per goroutine.

---

## License

MIT — see [LICENSE](LICENSE).
