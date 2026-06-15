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

Add `WithConcurrency(n)` to decode across cores while still streaming: workers
decode whole row groups ahead of the consumer into a small look-ahead window, and
`Read` delivers rows **in file order**. This works with `Where(...)` too. ~3.7×
faster on a 20-row-group file. Memory stays bounded but rises to roughly
`concurrency × rows-per-row-group` (the look-ahead), so it's most useful on files
with many row groups; needs a concurrent-safe `io.ReaderAt` (`*os.File`,
`*bytes.Reader`, S3 — same as the materialize path). A `Reader` is still single-
consumer: call `Read` from one goroutine.

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

## Column projection (read only the columns you need)

To read a subset of a wide file's columns, decode into a struct that contains
only those fields. Columns your Go type doesn't map to are **skipped in the read
pipeline** — no page fetch, no decompression, no decode — not merely ignored:

```go
// File has 50 columns; you only need two.
type Slim struct {
    ID   int64  `parquet:"id"`
    Name string `parquet:"name"`
}

rows, err := parquetfast.UnmarshalFile[Slim]("wide.parquet") // reads 2 columns, not 50
```

This is on by default and needs no flag — just use a narrower struct. Reading 2
scalar columns from a wide record reads ~10% of the bytes and is ~3.7× faster
than decoding the full struct. The result is identical to decoding the full
struct and discarding the extra fields.

Notes:
- A compound field that *is* present (a map, list, or struct-valued field) reads
  its whole subtree; projection drops whole unreferenced columns/fields, not
  individual leaves inside a kept container.
- An optional-struct field that is present reads all of its descendant columns
  (its presence is detected from them).
- `WithoutColumnProjection()` turns it off and reads every column (same result,
  more work) — useful only for debugging.

## Filtering rows (predicate pushdown)

Pass `Where(...)` to keep only matching rows. Row groups whose column statistics
(min/max, null count) or bloom filter prove they can't match are **skipped
entirely** — their pages are never fetched, decompressed, or decoded:

```go
rows, err := parquetfast.UnmarshalFile[Event]("events.parquet",
    parquetfast.Where(
        parquetfast.Col("ts").Between(start, end),   // time range
        parquetfast.Col("region").Equal("eu"),       // AND
    ),
)
```

Build leaf predicates with `Col(path...)` and one of `Equal`, `NotEqual`, `Less`,
`LessOrEqual`, `Greater`, `GreaterOrEqual`, `Between`, `In`, and combine them with
`And` / `Or` / `Not` (nestable). Multiple predicates in one `Where` are ANDed.
Supported value types: bool, all int/uint widths, float32/64, string, `[]byte`,
and `time.Time` (against TIMESTAMP/DATE columns). The filter column **need not be
a field of your struct** — you can filter on a column you don't decode.

```go
// status != "ok" AND NOT(region == "eu" AND tier < 2)
parquetfast.Where(
    parquetfast.Col("status").NotEqual("ok"),
    parquetfast.Not(parquetfast.And(
        parquetfast.Col("region").Equal("eu"),
        parquetfast.Col("tier").Less(int64(2)),
    )),
)
```

`Not(...)` is normalized at compile time by pushing the negation down to the
leaves (De Morgan: `!(a AND b)` → `!a OR !b`, `!(x == v)` → `x != v`,
`!Between` → `< lo OR > hi`), so pruning still applies. As in SQL, NULL never
matches a value predicate (so `NotEqual`/`Not` exclude NULL rows).

`In(...)` is an Or of equality checks, so it prunes per value (statistics +
bloom) and composes with `Not` (`Not(Col("x").In(a, b))` → `x != a AND x != b`):

```go
parquetfast.Where(parquetfast.Col("region").In("us", "eu", "apac"))
```

```go
// region == "eu" AND (status == "error" OR latency_ms > 1000)
rows, err := parquetfast.UnmarshalFile[Event]("events.parquet",
    parquetfast.Where(
        parquetfast.Col("region").Equal("eu"),
        parquetfast.Or(
            parquetfast.Col("status").Equal("error"),
            parquetfast.Col("latency_ms").Greater(int64(1000)),
        ),
    ),
)
```

Pruning recurses through the tree: an `And` keeps a row group/page only if every
child can match; an `Or` keeps it if any child can (and unions their surviving
page ranges).

How it works (coarse to fine):
- **Row-group pruning** — before reading any pages, each group's min/max and null
  count are compared to the predicate (`Equal` also consults the bloom filter, if
  present), skipping groups that can't match.
- **Page pruning + seek** — within a surviving group, the column's page index
  (per-page min/max + first-row offsets) identifies which pages can match; the
  reader seeks straight to those pages (`SeekToRow`) and skips the rest, so
  pruned pages are never fetched or decoded. Multi-column predicates intersect
  their surviving page ranges.
- **Row filtering** — the surviving rows are decoded and only matching ones are
  returned, in file order.

This works through every read API — `Unmarshal`, `UnmarshalBytes`,
`UnmarshalFile`, and the streaming `Reader` (`NewReader[T](r, size, Where(...))`).

Effectiveness scales with how well the data is clustered on the filter column.
On a 1M-row file (20 row groups) where the predicate selects ~one group:

| | time | bytes | allocs |
|---|---:|---:|---:|
| full decode | 55.8 ms | 39.5 MB | 1.00 M |
| `Where(Col("id").Between(…))` | **3.1 ms** | **2.1 MB** | **41 k** |

That's **~18× faster, −95% bytes, −96% allocs** — and against a range-capable
`io.ReaderAt` (e.g. S3) it also fetches ~95% fewer bytes off the wire, since
pruned groups' pages are never read.

Page pruning sharpens this *within* a row group. On a single row group split into
40 pages, a narrow `Between` over the (clustered) column read **4.3%** of the
bytes of a full decode and ran ~43× faster — because only the matching pages are
fetched and decoded. The win scales with how well the data is clustered on the
filter column.

When the filter column is **sorted** (parquet-go marks the page index ascending
automatically when the data is written in order), page selection uses a binary
search over the page index instead of scanning every page — O(log pages) rather
than O(pages), with identical results. On a **2 GiB** sorted file (16.7M rows), a
range query returning 101 rows read **0.15% of the file (3 MiB) in 3 ms** — only
the footer, the surviving row group's page index, and the matching page.

NULL values never match a value predicate. Add `WithConcurrency(n)` to filter
across workers — each surviving row group is filtered independently and the
results are concatenated in file order (≈6× faster when many groups survive).

## Reading from S3 / remote storage

parquet-go reads the **footer first** and then only the byte ranges of the pages
it actually needs (via `io.ReaderAt.ReadAt`). So if you back the reader with
ranged GETs, a decode downloads only the bytes it uses — and combined with
**column projection** (decode a narrow struct) and **`Where(...)` filtering**
(prune row groups), reading a few columns or a selective slice of a large remote
file fetches a small fraction of it. The library stays dependency-free; you
supply the transport via `ReaderAtFunc`:

```go
ra := parquetfast.ReaderAtFunc(func(p []byte, off int64) (int, error) {
    out, err := s3c.GetObject(ctx, &s3.GetObjectInput{
        Bucket: aws.String(bucket),
        Key:    aws.String(key),
        Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)),
    })
    if err != nil { return 0, err }
    defer out.Body.Close()
    return io.ReadFull(out.Body, p)
})

rows, err := parquetfast.Unmarshal[Event](ra, objectSize,
    parquetfast.Where(parquetfast.Col("ts").Between(start, end)), // prune row groups
    parquetfast.WithOptimisticRead(),                            // footer in one GET
    parquetfast.WithReadBufferSize(4<<20),                       // fewer, larger GETs
)
```

Tuning options (each forwards a parquet-go read option):
- `WithOptimisticRead()` — read the footer region in a single request at open.
- `WithReadBufferSize(n)` — larger buffer ⇒ fewer, larger range GETs (try `4<<20`).
- `WithAsyncReads()` — prefetch pages in the background to hide GET latency.
- `WithFileOptions(...)` — pass any other `parquet.FileOption` (e.g. bloom prefetch).

Measured against an in-memory reader that counts ranged reads (a stand-in for
S3): a 2-column projection fetched **21%** of the bytes of a full decode, a
selective `Where` fetched **19%**, and `WithOptimisticRead` cut open-time round
trips from 10 to 7. If `WithConcurrency(n>1)` is used, `ReadAt` must be safe for
concurrent calls (S3 `GetObject` is). `Where(...)` filtering works through every
read API, including the streaming `Reader`, and row-group + page pruning mean a
filtered remote read fetches only the footer, the page index, and the surviving
pages.

---

## How it works

Two stages: **Plan** (once) then **Apply** (per row).

### Stage 1 — Plan (built once, cached)

`Compile(reflect.Type, *parquet.Schema, skip)` walks the Go struct once and, for
every leaf field, records a compact descriptor: the **parquet leaf-column index**
it binds to, the field's **byte offset** within the struct, and a **kind enum**
selecting the typed write. Fields spanning multiple columns (maps, lists,
optional structs) get a closure.

**The compiled plan is cached and reused.** The cache is process-wide,
concurrency-safe, and keyed on `(Go type, schema hash, null-column hash)`, so the
reflection walk runs **once per distinct shape for the whole process** — not once
per call. Decoding 10,000 files with the same `T` and schema compiles one plan
and reuses it for all of them; `Unmarshal`, `UnmarshalFile`, `UnmarshalBytes`,
and `Reader` all share it. You don't manage this — `Compile` looks up the cache
first and only builds on a miss.

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
| `[]string, []bool, []int…, []uint16/32/64, []float…` | typed inline | required elements; both `repeated` and `,list` layouts |
| `[]*string, []*bool, []*int…, []*float…` | typed inline | **nullable elements**: null → nil, positions preserved |
| `[]time.Time` | typed inline | TIMESTAMP/DATE elements |
| **Struct slices** | | |
| `[]Struct` | typed fast path | `RegisterStructList[T]`, else `reflect.MakeSlice` |
| **Maps — `map[K]primitive`** | | |
| `map[string]string`, `map[string]int64`, `map[int64]float64` | typed inline | |
| other `map[K]V` primitive | reflect | K ∈ {string,int32,int64,float64}; V primitive incl. bool |
| **Maps — `map[K]Struct`** | typed fast path | `RegisterStructValuedMap[K,V]`, else reflect |
| **Maps — `map[K]time.Time`** | reflect | time value is a single leaf |
| **Nested maps — `map[K1]map[K2]V`** | reflect | inner V primitive |

> **List elements & producer compatibility.** `[]T` drops null elements; use
> `[]*T` to keep them (null → nil, positions preserved). The list element node is
> resolved *structurally*, so files whose element is named `item`/`array`
> (parquet-cpp and some Spark/Presto output) decode correctly — parquet-go's own
> `GenericReader` assumes the spec-default name `element` and silently returns
> empty lists for those files.

### Not supported (errors at `Compile`)

| Go field type | Why | Use instead |
|---|---|---|
| `[]*Struct` | pointer-element struct slices | `[]Struct` |
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

The gain is **workload-dependent**, so here is a matrix across data-model shapes
rather than a single headline number. Each shape decodes 100,000 rows over 2 row
groups (snappy), comparing `parquet-go`'s reflection `GenericReader` against
`parquet-go-fast` — both **streaming with a reused 4096-row buffer** (the
apples-to-apples comparison), with all typed registrations applied. Deltas are
parquet-go-fast vs parquet-go (negative = better). Apple M4 Pro, Go 1.26,
parquet-go v0.30.1. Synthetic data — reproduce with the command below.

| Shape | time | bytes | allocs |
|---|---:|---:|---:|
| Flat scalars (10 fields) | **−27%** | −3% | ~0% |
| Scalars + optionals + `[]byte` | **−25%** | −2% | ~0% |
| Primitive maps (`map[string]string`, …) | **−59%** | +64% | −50% |
| Struct-valued map (12 entries/row) | **−42%** | +58% | −20% |
| Deep nested structs (3 levels, optional) | **−22%** | −1% | ~0% |
| Struct slices (`[]Struct`, 8/row) | **−24%** | +14% | −9% |
| `time.Time` heavy (4 time cols + list) | **−54%** | −66% | −80% |
| Wide mixed (all features at once) | **−37%** | +15% | −19% |

What this says:

- **Latency is always lower — between −22% and −59%**, never slower. It is *not*
  a constant ~47%; expect roughly −25% on flat/nested records and −40% to −60%
  on map- and time-heavy ones (where avoiding reflection helps most).
- **Allocation count is equal-to-much-lower** — about the same on flat/nested
  shapes (both decoders pay the same string/`[]byte` content copies), and
  sharply lower on map-heavy (−20% to −50%) and `time.Time`-heavy (−80%) shapes.
- **Memory bytes are mixed *in the streaming matrix above*.** Lower on scalar
  and `time.Time` shapes (the time fast path is a big win: −66% bytes / −80%
  allocs), but higher on map-heavy shapes (up to +64%). The reason is specific:
  when streaming into a **reused** buffer, `GenericReader` reuses the existing
  map's storage in each reused slot (it only allocates a map when the slot is
  nil), whereas parquet-go-fast zeroes each slot first (to avoid leaking a prior
  row's data) and so `make`s a fresh map per row. That's extra GC *churn* (total
  bytes allocated), **not** higher peak/live memory — peak stays bounded by the
  batch for both. It also **only affects the streaming `Reader`**: with
  `UnmarshalBytes`/`UnmarshalFile` every destination slot is fresh, neither
  reader can reuse, and parquet-go-fast wins on bytes too (see below). So for the
  common materialize-all case, memory is strictly lower.

The `time.Time` row is the standout: parquet-go decodes each timestamp through
reflection (≈817k allocs here), while parquet-go-fast reconstructs it with a
single typed `time.Unix` (≈167k).

### Materialize-all on large data (the common case)

The matrix above is *streaming* (reused buffer). Most callers instead use
`UnmarshalBytes` / `UnmarshalFile`, which return the whole `[]T`. Compared to
`GenericReader` reading the whole file into a slice — 500k rows of the wide mixed
record over 10 row groups, Apple M4 Pro:

| API | time | bytes | allocs |
|---|---:|---:|---:|
| `parquet-go` `GenericReader` (materialize) | 2623 ms | 5158 MB | 50.4 M |
| **`UnmarshalBytes`** | **1304 ms** | **2401 MB** | **35.7 M** |
| **`UnmarshalBytes` + `WithConcurrency(0)`** | **346 ms** | 2430 MB | 35.8 M |
| `UnmarshalFile` | 1379 ms | 2400 MB | 35.7 M |
| `UnmarshalFile` + `WithConcurrency(0)` | 349 ms | 2429 MB | 35.8 M |

When both sides materialize the full result, parquet-go-fast wins on **all three
axes** (−50% time, −53% bytes, −29% allocs) — the streaming "+bytes on map-heavy
shapes" caveat does not apply here, because `GenericReader`'s materialize path is
much heavier per row. Concurrency adds a further 3.8× (−87% / 7.6× vs
`GenericReader`) on this 10-row-group file.

### The production sweet spot: sparse, wide, map-heavy records

The biggest wins are on records shaped like real telemetry/analytics rollups:
many columns, a high-cardinality `map[string]Struct`, `[]byte` histogram blobs,
and a **long tail of optional columns that are entirely null per file** (features
most rows don't use), including null sub-structs *inside* the map. 200k such
records, 47 leaf columns, 4 row groups, Apple M4 Pro:

| API | time | bytes | allocs |
|---|---:|---:|---:|
| `parquet-go` `GenericReader` (materialize) | 2052 ms | 4895 MB | 33.0 M |
| **`UnmarshalBytes`** | **770 ms** | **1711 MB** | **18.8 M** |
| **`UnmarshalBytes` + `WithConcurrency(0)`** | **275 ms** | 1720 MB | 18.8 M |

That's **−62% time, −65% bytes, −43% allocs** vs `GenericReader`, and **7.5×
(−87%)** with concurrency. The reflection reader pays to decode the all-null tail
*per map entry* (8×/row here); parquet-go-fast never allocates for it, and its
null-column elision also skips those pages outright (~15% of the time saving).
This mirrors what the same approach achieves on production data
(`pqtWorkload`-style files): −67% time / −85% bytes / −47% allocs vs the
reflection reader.

Reproduce:

```sh
go test -run='^$' -bench='BenchmarkShape' -benchmem -benchtime=10x
go test -run='^$' -bench='BenchmarkLargeUnmarshal|BenchmarkSparseWide' -benchmem -benchtime=5x
go test -run='^$' -bench='BenchmarkConcurrentDecode|BenchmarkStreamConcurrent|BenchmarkProjection|BenchmarkFilter|BenchmarkPagePruning|BenchmarkPlanApply' -benchmem
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
- **`time.Time`** decodes to the absolute instant in **UTC** (parquet stores an
  epoch value; the column's adjusted-to-UTC flag affects only display). Compare
  decoded times with `.Equal`, not `==`/`reflect.DeepEqual` — Go has multiple
  internal representations for the same instant. Float/byte-array timestamp
  encodings are a `Compile` error rather than a silent mis-decode.
- **Concurrency.** A compiled `Plan` is read-only and safe to share across
  goroutines. `WithConcurrency(n>1)` decodes one file across cores — on
  `Unmarshal`/`UnmarshalBytes`/`UnmarshalFile` (with or without `Where`) and on
  the streaming `Reader` — and needs a concurrent-safe `io.ReaderAt`; results are
  always in file order. A single `Reader` is one consumer: call `Read` from one
  goroutine (it manages its own internal workers).

---

## Testing

The suite encodes with `parquet-go`'s `GenericWriter` and decodes back with this
library (`reflect.DeepEqual` gate), covering every supported shape in isolation
plus two production-shaped records from an unrelated domain (e-commerce /
logistics rollups: a wide record with a high-cardinality struct-valued map, a
nested map, a struct slice, optional struct chains, and `[]byte` blobs). Scale
tests generate **multi-row-group files** on disk (a one-million-row concurrent
soak plus 250k-row streaming runs) and stream-decode them end to end, checking
order-independent aggregates so a dropped, duplicated, or corrupted row is caught
without holding every row in memory. All fixtures are synthetic.

```sh
go test ./...            # full suite, incl. the scale tests
go test -short ./...     # skips the scale tests (sub-second)
```

### Conformance (Apache spec corpus)

Beyond the synthetic suite, the library is checked against
[apache/parquet-testing](https://github.com/apache/parquet-testing) — golden
files written by parquet-mr/Java, parquet-cpp, parquet-rs, Impala, Spark and
Presto, exercising encodings and edge cases our own writer never emits
(DELTA\_\*, BYTE\_STREAM\_SPLIT, RLE\_DICTIONARY, Float16, INT96, decimals,
LZ4/brotli, legacy 2-level lists, maps without required keys, null pages, …).
Every file the reference reader can open decodes with a matching row count, and a
curated set is compared value-for-value against `parquet-go`'s reference reader.
The corpus is not vendored; point `PARQUET_TESTING_DIR` at a checkout:

```sh
git clone https://github.com/apache/parquet-testing
PARQUET_TESTING_DIR=parquet-testing/data go test -run TestConformance ./...
```

## License

MIT — see [LICENSE](LICENSE).
