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

Use the same `parquet:"..."` struct tags `parquet-go`'s writer uses, then make
one call that returns `[]T`:

```go
type Row struct {
    Name   string            `parquet:"name"`
    Count  int64             `parquet:"count"`
    Active bool              `parquet:"active"`
    Tags   []string          `parquet:"tags"`
    Labels map[string]string `parquet:"labels"`
}

// From a file on disk:
rows, err := parquetfast.UnmarshalFile[Row]("data.parquet")

// From an in-memory file:
rows, err := parquetfast.UnmarshalBytes[Row](data)

// From any io.ReaderAt of a known size:
rows, err := parquetfast.Unmarshal[Row](r, size)
```

That's it — all the row-group iteration and batched reads happen inside the
library. See [Large files](#large-files) if you'd rather stream than hold the
whole result set in memory.

> If your rows contain nested structs, struct slices, or struct-valued maps,
> register them once in `init()` for a faster, allocation-light decode path —
> see [Typed fast-path registration](#typed-fast-path-registration). It's
> optional: everything decodes correctly without it.

## Large files

For files too big to hold the whole `[]T` in memory, stream with a reused
destination buffer instead:

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

`Read` fills the buffer, returns how many rows it wrote, and reports `io.EOF`
once the file is exhausted. Working memory stays bounded by `len(buf)` regardless
of file size.

## Concurrent decoding

Decode one file across multiple cores by fanning its row groups out to worker
goroutines — opt in with `WithConcurrency`:

```go
rows, err := parquetfast.UnmarshalFile[Row]("big.parquet", parquetfast.WithConcurrency(0))
```

`WithConcurrency(n)` uses `n` workers; `n <= 0` means `GOMAXPROCS`; the default
(`1`) is sequential. Each worker decodes whole row groups into disjoint regions
of the result, so there's no locking on the hot path. **Speedup scales with the
number of row groups** — a single-row-group file is always decoded sequentially.
On a 16-row-group fixture this is ~3× faster than sequential.

When `n > 1`, the `io.ReaderAt` passed to `Unmarshal` must support concurrent
`ReadAt` calls. `*os.File` and `*bytes.Reader` do, so `UnmarshalFile` and
`UnmarshalBytes` are always safe; only a custom `io.ReaderAt` needs checking.

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
| `time.Time` | typed inline | TIMESTAMP (ms/µs/ns) or DATE → UTC instant |
| **Optional scalars (`*T`)** | | |
| `*string, *bool, *int…, *uint…, *float…` | typed inline | nil on null/absent |
| `*[]byte` | typed inline | optional BYTE_ARRAY |
| `*time.Time` | typed inline | nil on null/absent |
| **Structs** | | |
| `struct{…}` | typed inline | embedded at parent offset |
| `*struct{…}` | typed fast path | `RegisterStructAlloc[T]`, else `reflect.New` |
| **Primitive slices** | | |
| `[]string, []bool, []int…, []uint16/32/64, []float…` | typed inline | both `repeated` and `,list` layouts |
| `[]time.Time` | typed inline | TIMESTAMP/DATE elements |
| **Struct slices** | | |
| `[]Struct` | typed fast path | `RegisterStructList[T]`, else `reflect.MakeSlice` |
| **Maps — `map[K]primitive`** | | |
| `map[string]string`, `map[string]int64`, `map[int64]float64` | typed inline | |
| other `map[K]V` primitive | reflect | K ∈ {string,int32,int64,float64}; V primitive incl. bool |
| **Maps — `map[K]Struct`** | typed fast path | `RegisterStructValuedMap[K,V]`, else reflect |
| **Maps — `map[K]time.Time`** | reflect | time value is a single leaf |
| **Nested maps — `map[K1]map[K2]V`** | reflect | inner V primitive |

### Not supported (errors at `Compile`)

| Go field type | Why | Use instead |
|---|---|---|
| `[]*T` | pointer-element slices | `[]T` |
| `map[K][]V` | mixed nesting: two repetition levels with no struct dispatch boundary | `map[K]struct{ Items []V }` |
| `[]map[K]V` | same | `[]struct{ M map[K]V }` |
| `map[K][]Struct` | same | `map[K]struct{ Items []Struct }` |
| `map[K1]map[K2]Struct` | nested struct-valued maps | `map[K1]struct{ Inner map[K2]Struct }` |

The fix for every mixed-nesting case is the same: wrap the inner collection in a
named struct, which gives the decoder a struct boundary between the two
repetition levels. For example, instead of `map[string][]int64` use:

```go
type Bucket struct { Values []int64 `parquet:"values"` }
M map[string]Bucket `parquet:"m"`
```

---

## Typed fast-path registration

All three registries are **optional** performance knobs. Unregistered types
decode correctly via a reflect fallback; registering swaps in a generic-typed
path that avoids `reflect.New` / `reflect.MakeSlice` / `reflect.MakeMapWithSize`
/ `SetMapIndex` per entry.

| Field shape | Register | What it replaces |
|---|---|---|
| `*Struct` (optional struct) | `RegisterStructAlloc[Struct]()` | `reflect.New` per row |
| `[]Struct` (struct slice) | `RegisterStructList[Struct]()` | `reflect.MakeSlice` + `Value.Index` per entry |
| `map[K]Struct` (struct-valued map) | `RegisterStructValuedMap[K, Struct](keyDecode)` | `reflect.New` + `SetMapIndex` per entry |

Worked example — a record using all three, registered once in `init()`, then
decoded with a single call:

```go
type Address struct {
    City string `parquet:"city"`
    Zip  string `parquet:"zip"`
}

type LineItem struct {
    SKU string `parquet:"sku"`
    Qty int64  `parquet:"qty"`
}

type Container struct {
    Name  string `parquet:"name"`
    Count int64  `parquet:"count"`
}

type Order struct {
    ID         string               `parquet:"id"`
    ShipTo     *Address             `parquet:"ship_to,optional"`  // optional struct
    Items      []LineItem           `parquet:"items"`             // struct slice
    Containers map[string]Container `parquet:"containers"`        // struct-valued map
}

func init() {
    parquetfast.RegisterStructAlloc[Address]()
    parquetfast.RegisterStructList[LineItem]()
    parquetfast.RegisterStructValuedMap[string, Container](
        func(v parquet.Value) string { return string(v.ByteArray()) },
    )
}

func loadOrders(path string) ([]Order, error) {
    return parquetfast.UnmarshalFile[Order](path) // registrations apply automatically
}
```

The `keyDecode` callback for `RegisterStructValuedMap` decodes the map key from a
`parquet.Value` (you know the key's parquet kind): `v.ByteArray()` for string
keys, `v.Int64()` for `int64`, `v.Int32()` for `int32`, `v.Double()` for
`float64`.

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

The gap is workload-dependent. On a deliberately extreme record (a wide
e-commerce rollup with a high-cardinality `map[string]Struct`, a nested map, a
struct slice, and several `[]byte` blobs — `BenchmarkMerchantDecode`), streaming
is **−37% time, −19% allocations**, though total bytes run slightly higher than
`GenericReader` because each row allocates many small maps/structs either way.
Time and allocation count win across the board; raw bytes are close on the most
map-dense shapes.

Reproduce:

```sh
go test -run='^$' -bench='BenchmarkDecode|BenchmarkMerchantDecode|BenchmarkPlanApply' -benchmem
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
- **`time.Time`** decodes to the absolute instant in **UTC** (parquet stores an
  epoch value; the column's adjusted-to-UTC flag affects only display). Compare
  decoded times with `.Equal`, not `==`/`reflect.DeepEqual` — Go has multiple
  internal representations for the same instant. Float/byte-array timestamp
  encodings are a `Compile` error rather than a silent mis-decode.
- **Concurrency.** A compiled `Plan` is read-only and safe to share across
  goroutines. `Unmarshal` with `WithConcurrency(n>1)` decodes one file across
  cores (needs a concurrent-safe `io.ReaderAt`; see
  [Concurrent decoding](#concurrent-decoding)). A single `Reader` is not safe for
  concurrent use; create one per goroutine.

---

## Testing

The suite encodes with `parquet-go`'s `GenericWriter` and decodes back with this
library (`reflect.DeepEqual` gate), covering every supported shape in isolation
plus two production-shaped records from an unrelated domain (e-commerce /
logistics rollups: a wide record with a high-cardinality struct-valued map, a
nested map, a struct slice, optional struct chains, and `[]byte` blobs). Two
scale tests generate **one-million-row, multi-row-group files** on disk and
stream-decode them end to end, checking order-independent aggregates so a
dropped, duplicated, or corrupted row is caught without holding every row in
memory. All fixtures are synthetic.

```sh
go test ./...            # full suite, incl. the million-row scale tests
go test -short ./...     # skips the million-row tests
```

## License

MIT — see [LICENSE](LICENSE).
