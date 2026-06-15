# parquet-go-fast

A high-performance, reflection-free-on-the-hot-path **parquet decoder** for Go. It
reads parquet files into Go structs far faster — and with orders of magnitude
fewer allocations — than the reflection-driven reader in
[`parquet-go/parquet-go`](https://github.com/parquet-go/parquet-go).

> 📊 **Benchmarks:** see [Performance](#performance) — full-read, projection, and
> filtered comparisons vs parquet-go, arrow-go, and DuckDB→Go across four dataset
> shapes (flat numeric, string-heavy, nested list, deeply nested).

```go
import parquetfast "github.com/savak1990/parquet-go-fast"

rows, err := parquetfast.UnmarshalFile[MyRow]("data.parquet") // []MyRow
```

**Who it's for.** Reach for it when your Go code needs parquet as native typed rows
to *operate on* — ETL and data enrichment, joining or augmenting row-structured
records, event/log replay, backfills, or feeding rows into a Go service — rather than
running analytical queries over the file (use a query engine like DuckDB for that).
It's pure Go on the hot path (no cgo, no Arrow runtime), so it drops into any Go binary.

It is **decode-only**, depends only on `parquet-go` (no fork, no `replace`), and
reads files written by any spec-conformant writer (parquet-go, Arrow, Spark,
DuckDB, pandas, …). It compiles the `(Go type, file schema)` mapping
**once** and then decodes every row through precompiled, typed `unsafe.Pointer`
writes — no per-row reflection.

## Install

```sh
go get github.com/savak1990/parquet-go-fast
```

## Usage

Use the same `parquet:"..."` struct tags that `parquet-go`'s writer reads.

### Decode a whole file

```go
type Row struct {
    Name   string            `parquet:"name"`
    Count  int64             `parquet:"count"`
    Labels map[string]string `parquet:"labels"`
}

rows, err := parquetfast.UnmarshalFile[Row]("data.parquet") // from disk
rows, err := parquetfast.UnmarshalBytes[Row](data)          // from memory
rows, err := parquetfast.Unmarshal[Row](r, size)            // any io.ReaderAt
```

### Stream large files (bounded memory)

A `Reader` decodes in caller-sized batches instead of holding the whole `[]T`, so
memory stays bounded regardless of file size. Three ways to consume it, lowest- to
highest-level:

**`Read` — fill a reused buffer** (the primitive):

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

**`All` — range over batches** (Go 1.23 iterator; one in-process consumer):

```go
for batch, err := range rd.All(4096) {
    if err != nil { return err }
    for i := range batch { process(batch[i]) }
}
```

The yielded slice is reused on the next iteration — copy any element you keep.

**`Chan` — a background producer** (decode overlaps a slower consumer — a DB, a
network sink — across a goroutine boundary):

```go
ch, wait := rd.Chan(ctx, 4096)
for batch := range ch {        // each batch is freshly allocated, safe to retain
    for i := range batch { process(batch[i]) }
}
if err := wait(); err != nil { return err } // terminal error (nil on clean EOF)
```

Cancel `ctx` to stop early. All three deliver rows **in file order**, and all
compose with `WithConcurrency` — the decode then runs across row groups in parallel
while delivery stays ordered (see [Decode across cores](#decode-across-cores)).

### Read only some columns (projection)

Decode into a struct with just the fields you need; unreferenced columns are
skipped in the read pipeline (no fetch, decompress, or decode):

```go
type Slim struct {
    ID   int64  `parquet:"id"`
    Name string `parquet:"name"`
}
rows, err := parquetfast.UnmarshalFile[Slim]("wide.parquet") // reads 2 columns, not 50
```

### Filter rows (predicate pushdown)

```go
rows, err := parquetfast.UnmarshalFile[Event]("events.parquet",
    parquetfast.Where(
        parquetfast.Col("ts").Between(start, end),
        parquetfast.Col("region").Equal("eu"), // multiple predicates are ANDed
    ))
```

Build leaf predicates with `Col(path...)` and one of `Equal`, `NotEqual`, `Less`,
`LessOrEqual`, `Greater`, `GreaterOrEqual`, `Between`, `In`; combine with `And`,
`Or`, `Not` (nestable). The result contains **only matching rows**, in file order.
Row groups and pages that can't match (column stats / bloom filter) are skipped.
The filter column need not be a field of your struct.

### Decode across cores

```go
rows, err := parquetfast.UnmarshalFile[Row]("big.parquet", parquetfast.WithConcurrency(0)) // 0 = GOMAXPROCS
```

Results stay in file order. Works on `Unmarshal*` (with or without `Where`) and on
the streaming `Reader`; speedup scales with the number of row groups. Needs a
concurrent-safe `io.ReaderAt` (`*os.File`, `*bytes.Reader`, and S3 readers qualify).

### Read from S3 / remote storage

```go
ra := parquetfast.ReaderAtFunc(func(p []byte, off int64) (int, error) {
    // issue an S3 GetObject with a Range header for [off, off+len(p))
    return n, err
})
rows, err := parquetfast.Unmarshal[Row](ra, size, parquetfast.WithOptimisticRead())
```

Parquet's footer-first layout means a projected/filtered read fetches a small
fraction of the object. Tune with `WithReadBufferSize`, `WithAsyncReads`, and
`WithFileOptions`.

### Nested types: optional registration

Nested structs, struct slices, and struct-valued maps decode correctly out of the
box (a reflect fallback). For an allocation-light typed path, register them once:

```go
func init() {
    parquetfast.RegisterStructAlloc[Address]()                      // *Struct fields
    parquetfast.RegisterStructList[LineItem]()                      // []Struct fields
    parquetfast.RegisterStructValuedMap[string, Product](keyDecode) // map[K]Struct fields
}
```

## Architecture

The whole design serves one goal: **get bytes from the file into Go struct fields
with the least possible work per value.**

**Compile once, then no reflection.** The first decode of a given `(Go type, file
schema)` walks both with reflection a single time and compiles a **plan**: a flat
list of scalar *setters* — `{column index, field byte-offset, type kind}` — plus one
closure per compound field (map, list, optional struct). After that, writing a value
is a typed `switch` that stores straight to `base + offset` through `unsafe.Pointer`
— no `reflect.Value`, no interface boxing, no per-field call on the hot path. Plans
are cached process-wide (keyed on type, schema, and null-column shape), so reflection
is paid once per process, not once per row.

**Scalar-only schemas take a columnar fast path — this is the speed story.** When the
struct has no maps, lists, or optional structs, the decoder skips parquet-go's row
reader (and its row-assembly step) entirely and reads **one column at a time**,
writing strided into the output structs. Numeric columns go **straight from the
page's typed buffer** — dictionary indices resolved against the dictionary's typed
values, nulls placed from the definition levels — with **no `parquet.Value` boxing
and one typed store per cell**. That is why full reads and projection beat even
arrow-go's columnar reader: we write directly into the final `[]struct`, whereas a
columnar reader still has to transpose its arrays into rows afterward.
Strings/bools/`time.Time`/optionals use a still-columnar but boxed read; any compound
field drops to the row path below, so nested schemas are simply unaffected.

**Nested schemas use the row path.** Maps, lists, and optional structs decode through
parquet-go's row reader, after which the plan's setters and closures populate the
struct. List columns are resolved **structurally** from the schema tree, so a plain
`[]T` reads any spec-conformant 3-level `LIST` no matter how its inner levels are
named (`element`, `item`, `array`).

**Concurrency is row-group parallelism.** `WithConcurrency(n)` fans the file's row
groups across `n` worker goroutines; each decodes whole row groups into a **disjoint
region** of the pre-allocated `[]T`, so there is no locking and no merge step. Speedup
therefore scales with the **number of row groups** — a single-row-group file can't be
split and runs sequentially. It requires a concurrency-safe `io.ReaderAt`;
`UnmarshalFile` and `UnmarshalBytes` both qualify.

**Skip work before decoding it — pruning at three levels.** A read does only the I/O
and decode the query actually needs:

- **Column** (projection) — only the columns bound to struct fields (plus any columns
  named in a predicate) are fetched, decompressed, and decoded; the rest of the file
  is never touched.
- **Row group** — each row group's min/max column statistics and bloom filters are
  tested against the predicate, and groups that can't contain a match are skipped
  whole.
- **Page** — within a surviving row group, per-page min/max statistics skip the data
  pages that can't match (a sorted column is binary-searched), so off-range pages are
  never read.

**Predicate filtering.** `Where(...)` builds a predicate tree — `Equal`, `Less`,
`Between`, `In`, … combined with `And`/`Or`/`Not` — whose leaves drive the pruning
above (a predicate column need not be a field of your struct). Rows that survive
pruning still need a row-level check: for a **scalar-only** output the filtered read
reuses the columnar path — it decodes the output columns once and evaluates the
predicate over the **already-decoded** values (so the filter column is never read
twice), keeps the matches, and parallelizes across row groups. A heavily-pruned scan
on a sorted column instead falls to a row-at-a-time path that seeks over the skipped
pages; anything else (string/bytes/`time.Time`/unsigned predicates, or compound
output) uses the row reader and filters as it goes.

## Performance

Benchmarks live in [`bench/`](bench/) (full methodology + reproduction). Each
reader is measured **end to end into a ready-to-use native row collection**, and
compared *within category* — full materialization vs columnar decode vs analytical
query are different amounts of work, so they're never mixed. Apple M4 Pro, Go 1.26.

**Readers compared — and exactly what we run for each:**

| Reader | Library / mechanism | Output |
|---|---|---|
| **parquet-go-fast** | this library — `UnmarshalBytes` / `WithConcurrency` | Go `[]struct` |
| parquet-go | [`parquet-go/parquet-go`](https://github.com/parquet-go/parquet-go) `GenericReader` (reflection) | Go `[]struct` |
| arrow-go | [`apache/arrow-go`](https://github.com/apache/arrow-go) `pqarrow` — pure-Go **columnar** read + our transpose to rows | Arrow arrays → `[]struct`\* |
| DuckDB → Go | [`marcboeker/go-duckdb`](https://github.com/marcboeker/go-duckdb) (cgo) over `database/sql` — `Scan` into structs | Go `[]struct` |

\* Only parquet-go-fast and parquet-go return a Go `[]struct` natively. **arrow-go
returns columnar Arrow arrays** — the "→ rows" numbers are *our* transpose on top,
and for string columns those values alias Arrow's buffer (views), not independent
Go strings (which is why its allocation counts look so low). **DuckDB → Go** is the
real in-process cgo driver going through `database/sql` (the per-cell `Scan` is the
bulk of its allocations) — not the CLI. The DuckDB/ClickHouse *CLI* appears only in
[`bench/sql/engines.sh`](bench/sql/engines.sh) for scalar analytical queries that
never materialize rows — not in any table here.

Every table reports **Time** (ns/op → ms), **Mem** (B/op) and **Alloc** (allocs/op)
from `go test -benchmem`, hot cache, min of repeats. **Returns** names what the
reader actually hands back (a Go `[]struct`, Arrow columns, or boxed values).
⚠️ **DuckDB → Go's Mem/Alloc count only Go-heap allocations** the Go runtime sees
(the `database/sql` `Scan` destinations + driver row buffers); the DuckDB C++ engine's
native (malloc) working set is invisible to `benchmem`, so its real footprint is
higher than shown. arrow-go's buffers are Go-managed and *are* counted, but its
string values alias those buffers (views), which is why its alloc counts look tiny.

### NYC TLC yellow-taxi — flat, columnar-friendly analytical file

[NYC TLC yellow-taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
2.96 M rows × 19 columns, warm cache. This is a **clean, idiomatic,
columnar-friendly analytical file** — flat scalar columns (no maps, lists, or
optional structs), dictionary-encoded, zstd-compressed. It is the shape parquet is
best suited for, and the favorable case for any columnar reader (this library
included): nested/map-heavy schemas decode through the slower row path and are a
separate story (see [Architecture](#architecture)).

The decoded record — numeric/timestamp-heavy, one small string:

```go
type Taxi struct {
    VendorID            int32     `parquet:"VendorID"`
    PickupTime          time.Time `parquet:"tpep_pickup_datetime"`
    DropoffTime         time.Time `parquet:"tpep_dropoff_datetime"`
    PassengerCount      int64     `parquet:"passenger_count"`
    TripDistance        float64   `parquet:"trip_distance"`
    RatecodeID          int64     `parquet:"RatecodeID"`
    StoreAndFwdFlag     string    `parquet:"store_and_fwd_flag"`
    PULocationID        int32     `parquet:"PULocationID"`
    DOLocationID        int32     `parquet:"DOLocationID"`
    PaymentType         int64     `parquet:"payment_type"`
    FareAmount          float64   `parquet:"fare_amount"`
    Extra               float64   `parquet:"extra"`
    // …7 more float64 fee columns (mta_tax, tip_amount, total_amount, …)
}
```

**Full read — all 19 columns → rows.** arrow-go returns columnar Arrow arrays
(builds no per-row objects, so it does strictly less work), shown for reference.

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **181 ms** | 578 MB | 1.6 k | |
| arrow-go | Arrow columns | 310 ms | 978 MB | 193 k | columnar; no row structs |
| parquet-go-fast | Go `[]struct` | 472 ms | 568 MB | 1.7 k | |
| DuckDB → Go | Go `[]struct` | 2104 ms | 3.6 GB ⚠️ | 42 M | cgo mem under-counted |
| parquet-go | Go `[]struct` | 3089 ms | 5.7 GB | 24 M | |

**Projection — 5 of 19 columns → rows.** Read only the selected columns. `arrow-go
→ rows` reads them into Arrow then transposes (that transpose is in its time).

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **19 ms** | 119 MB | 0.8 k | |
| parquet-go-fast | Go `[]struct` | 49 ms | 102 MB | 0.7 k | |
| arrow-go → rows | Arrow→`[]struct` | 83 ms | 321 MB | 34 k | transpose included |
| DuckDB → Go | Go `[]struct` | 535 ms | 170 MB | 9.2 M | |
| parquet-go | Go `[]struct` | 1997 ms | 5.1 GB | 18 M | reads all 19, drops 14 |

Scaling (single-core, 1 / 5 / 10 cols): ours **10 / 49 / 99 ms** · arrow-go 17 / 83 /
190 · DuckDB 136 / 535 / 931 · parquet-go 1947 / 1997 / 2306.

**Filter — `fare_amount > 100` (7 995 matches) → rows.** parquet-go has no pushdown
(decode all, filter in Go); DuckDB pushes the predicate down; arrow-go has no
pushdown reader (N/A).

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **DuckDB → Go** | Go `[]struct` | **12 ms** | 1.2 MB ⚠️ | 24 k | pushdown; materializes only matches |
| parquet-go-fast (concurrent) | Go `[]struct` | 17 ms | 122 MB | 0.8 k | |
| parquet-go-fast | Go `[]struct` | 43 ms | 115 MB | 0.8 k | |
| arrow-go | — | — | — | — | no predicate-pushdown reader |
| parquet-go | Go `[]struct` | 1986 ms | 5.1 GB | 18 M | decode all, filter in Go |

More selective `trip_distance > 50` (412 matches): DuckDB **8.8 ms** (47 KB), ours-conc
16 ms, ours 43 ms, parquet-go 1995 ms.

#### Where we stand on this file

- ✅ **Fastest way to get Go structs out of parquet.** We win full reads — 1.7×
  faster than arrow-go's columnar read, which doesn't even build structs — and
  projection at every width (~1.7–4× faster than arrow-go→rows, ~10–25× faster
  than DuckDB→Go), with allocations in the **hundreds–thousands** vs millions.
- 🟡 **Selective filters: close, and now mixed.** A filtered read decodes the output
  columns once (typed), evaluates the predicate, and keeps the matches — ~40×
  faster than parquet-go. We trail DuckDB by ~3.5× single-core
  and **~1.4× with `WithConcurrency`** on the 7 995-match case (it still wins via
  SIMD eval and by materializing only the matches — note its **1.2 MB** vs our
  122 MB, since we decode the whole output column). On the very selective 412-match
  case DuckDB's pushdown keeps a wider lead.

### Open-Orca — string-heavy text (HuggingFace)

[Open-Orca/OpenOrca](https://huggingface.co/datasets/Open-Orca/OpenOrca),
~995 K rows × **4 string columns** (~1 GB). The opposite of taxi: all `BYTE_ARRAY`,
so the typed numeric gather doesn't apply (strings use the boxed columnar
fallback) and decode is dominated by copying bytes into Go strings. (`-benchtime=3x`.)
**This file is written as a single row group** (all 994,896 rows, by
`parquet-cpp-arrow`), so `WithConcurrency` has nothing to fan out and runs
single-core — concurrency scales with row-group count, and here there's only one
(cf. taxi 3, dbpedia 39, structwiki 87).

The decoded record — all strings:

```go
type OpenOrca struct {
    ID           string `parquet:"id"`
    SystemPrompt string `parquet:"system_prompt"`
    Question     string `parquet:"question"`
    Response     string `parquet:"response"`
}
```

**Full read — 4 strings → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **1618 ms** | 2.0 GB | 3.9 M | |
| parquet-go-fast | Go `[]struct` | 1645 ms | 2.0 GB | 3.9 M | |
| DuckDB → Go | Go `[]struct` | 2187 ms | 4.2 GB ⚠️ | 16 M | cgo mem under-counted |
| arrow-go → rows | Arrow→`[]struct` | 2289 ms | 5.8 GB | 27 k | strings alias Arrow buffer (views) |
| parquet-go | Go `[]struct` | 2300 ms | 4.2 GB | 6.9 M | |

**Projection — 2 of 4 → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** | Go `[]struct` | **962 ms** | 1.1 GB | 2.0 M | |
| parquet-go-fast (concurrent) | Go `[]struct` | 974 ms | 1.1 GB | 2.0 M | no gain (1 row group) |
| DuckDB → Go | Go `[]struct` | 1001 ms | 2.3 GB ⚠️ | 8.0 M | cgo mem under-counted |
| arrow-go → rows | Arrow→`[]struct` | 1011 ms | 2.9 GB | 13 k | string views |
| parquet-go | Go `[]struct` | 1154 ms | 2.3 GB | 5.0 M | |

**Filter — `system_prompt == …` (224 K matches) → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **DuckDB → Go** | Go `[]struct` | **544 ms** | 512 MB ⚠️ | 1.8 M | predicate pushdown |
| parquet-go-fast | Go `[]struct` | 920 ms | 1.4 GB | 454 k | |
| parquet-go-fast (concurrent) | Go `[]struct` | 928 ms | 1.4 GB | 454 k | no gain (1 row group) |
| arrow-go | — | — | — | — | no predicate-pushdown reader |
| parquet-go | Go `[]struct` | 2167 ms | 4.2 GB | 6.9 M | |

#### Where we stand on this file

- **We win full + projection, but modestly.** On strings the numeric blowout doesn't
  carry over — string decode is **byte-copy-bound** (every reader pays it) and can't
  use the typed gather. We lead full (1.6 s vs 2.2–2.3 s) and projection (962 ms),
  but by ~1.2–1.4×, not multiples.
- **arrow-go doesn't actually hand back a Go slice here.** It returns columnar Arrow
  arrays; `arrow-go → rows` is *our* transpose, and for strings the values **alias
  Arrow's buffer (views)** — not independent Go strings (invalid once the table is
  released). That's why its alloc count is 27 k vs our 3.9 M: it never *owns* the
  strings, whereas our `[]OpenOrca` is fully independent, GC-safe data. Read the
  *time* as the comparable figure.
- **String filters take the row path** (string predicate ≠ the numeric columnar
  filter); ~1.7× behind DuckDB's pushdown — far closer than numeric filters once were.
- **Concurrency gives nothing here** — the single row group (noted above) leaves
  `WithConcurrency` nothing to fan out, so it runs single-core.

### dbpedia embeddings — nested list + strings (HuggingFace)

[KShivendu/dbpedia-entities-openai-1M](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M),
38,462 rows × 3 strings + a 1536-dim OpenAI embedding stored as a `LIST<float64>`
(~350 MB). The **list makes the record non-scalar-only → the full read takes our
row path** — the honest counterpart to taxi, and the same class as map/list-heavy
production data. Projecting just the scalar metadata (dropping the embedding) flips
it back to the columnar path.

```go
type WikiEmbedding struct {
    ID    string    `parquet:"_id"`
    Title string    `parquet:"title"`
    Text  string    `parquet:"text"`
    Emb   []float64 `parquet:"openai"` // 1536-dim embedding (LIST)
}
```

**Full read — 3 strings + 1536-d embedding → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **73 ms** | 678 MB | 169 k | plain `[]float64`, no tag |
| parquet-go-fast | Go `[]struct` | 642 ms | 581 MB | 162 k | |
| DuckDB → Go | `[]struct` + boxed list | 636 ms | 1.6 GB ⚠️ | 60 M | list → `[]interface{}` (boxed); cgo mem under-counted |
| arrow-go → rows | Arrow→`[]struct` | 741 ms | 2.7 GB | 90 k | columnar → row transpose |
| parquet-go | Go `[]struct` | 1386 ms | 4.8 GB | 573 k | needs `openai,list` (else empty) |

**Projection — id + title (no embedding → columnar) → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **0.8 ms** | 6.8 MB | 79 k | |
| parquet-go-fast | Go `[]struct` | 3.2 ms | 6.2 MB | 79 k | |
| arrow-go → rows | Arrow→`[]struct` | 3.2 ms | 15 MB | 12 k | string views |
| parquet-go | Go `[]struct` | 7.8 ms | 14 MB | 195 k | |
| DuckDB → Go | Go `[]struct` | 12 ms | 13 MB ⚠️ | 308 k | cgo mem under-counted |

**Filter — `title < "M"` (22 634 matches) → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **0.7 ms** | 8.8 MB | 51 k | |
| parquet-go-fast | Go `[]struct` | 4.5 ms | 11 MB | 51 k | |
| DuckDB → Go | Go `[]struct` | 6.9 ms | 6.8 MB ⚠️ | 181 k | pushdown; cgo mem under-counted |
| parquet-go | Go `[]struct` | 8.1 ms | 14 MB | 195 k | |
| arrow-go | — | — | — | — | no predicate-pushdown reader |

#### Where we stand on this file

- **We win the nested full read** — concurrent **73 ms**, ~9× faster than DuckDB
  and arrow-go and ~19× parquet-go. The row path isn't a liability here: building Go
  `[]float64` slices *directly* beats arrow-go's columnar→row transpose and DuckDB's
  per-element `[]interface{}` boxing through `database/sql` (its **60 M** allocations).
- **Lists need no special tag with this library.** We declare the field as a plain
  `[]float64` and read it. **parquet-go requires the `,list` struct tag** to bind a
  3-level `LIST` column — with a bare `[]float64` it silently returns an empty slice
  (no error), an easy data-loss footgun. The number above is the *fair* parquet-go
  run, with `openai,list` set so it actually decodes the embedding (its 4.8 GB of
  allocations are why it's slowest).
- **DuckDB returns the embedding boxed.** Its driver yields the `LIST<double>` as a
  `[]interface{}` of `float64` — not a `[]float64`. We leave it boxed (converting it
  would be work no other reader's number includes) and report it as such.
- **Projection (drops the list) and string-range filter both go our way** —
  concurrent sub-millisecond, ahead of every engine here.

---

### structured-wikipedia — deeply nested, application-shaped (HuggingFace)

[wikimedia/structured-wikipedia](https://huggingface.co/datasets/wikimedia/structured-wikipedia),
177,499 rows (~354 MB), **19 top-level fields that flatten to 65 leaf columns**. The
**least** parquet-idiomatic file here and the closest to a real application data
model: optional structs, lists-of-structs, a `list<string>`, and a
struct-nested-in-a-struct. Nothing about this is columnar-friendly — the full read is
*all* row path. (arrow-go can only read it **columnar** — a hand-written nested→Go row
transpose this deep is bespoke and fragile — so it appears in the full table as a
columnar read and is left out of the projection/filter tables.)

The struct below maps a representative subset of the 19 fields — each nested type
shows the shape (optional struct, list-of-struct, `[]string`, struct-in-struct):

```go
type Article struct {
    Identifier  int64     `parquet:"identifier"`
    Name        string    `parquet:"name"`
    Abstract    string    `parquet:"abstract"`
    URL         string    `parquet:"url"`
    DateCreated time.Time `parquet:"date_created"`

    Image      *SWImage  `parquet:"image"`       // optional struct
    MainEntity *SWEntity `parquet:"main_entity"` // optional struct

    AdditionalEntities []SWAddEntity `parquet:"additional_entities"` // list<struct{ []string, … }>
    License            []SWLicense   `parquet:"license"`             // list<struct>

    Version *SWVersion `parquet:"version"` // struct → *SWEditor (struct-in-struct)
}

type SWImage   struct{ ContentURL string; Height, Width int64 }
type SWEntity  struct{ Identifier, URL string }                       // main_entity
type SWLicense struct{ Identifier, Name, URL string }
type SWAddEntity struct {
    Aspects    []string                                               // list<string> inside a list<struct>
    Identifier string
    URL        string
}
type SWVersion struct{ Comment string; Identifier int64; Editor *SWEditor }
type SWEditor  struct{ Name string; EditCount int64; Groups []string; IsAdmin bool }
```

**Full read — every nested field → rows.** parquet-go is given the `,list` tags it
needs so it actually decodes the lists (a bare `[]T` reads them empty — see below).
DuckDB returns nested columns boxed; arrow-go returns columnar (no row transpose).

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **77 ms** | 867 MB | 5.6 M | plain `[]T`, no tag |
| parquet-go-fast | Go `[]struct` | 505 ms | 798 MB | 5.6 M | |
| parquet-go | Go `[]struct` | 770 ms | 1.2 GB | 7.6 M | needs `,list` on every list field |
| DuckDB → Go | `[]struct` + boxed nested | 985 ms | 1.4 GB ⚠️ | 24 M | nested → map / `[]interface{}`; cgo mem under-counted |
| arrow-go | Arrow columns (nested) | 2473 ms | 8.1 GB | 2.8 M | all 65 leaf cols; no row transpose |

**Projection — `identifier` + `name` + `url` (scalar → columnar) → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **11 ms** | 67 MB | 499 k | |
| parquet-go-fast | Go `[]struct` | 43 ms | 62 MB | 499 k | |
| DuckDB → Go | Go `[]struct` | 63 ms | 80 MB ⚠️ | 1.6 M | cgo mem under-counted |
| parquet-go | Go `[]struct` | 334 ms | 682 MB | 1.7 M | |
| arrow-go | — | — | — | — | nested-schema leaf-index mapping bespoke (omitted) |

**Filter — `name < "A"` (79 003 matches) → rows**

| Reader | Returns | Time | Mem | Alloc | Note |
|---|---|--:|--:|--:|---|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **21 ms** | 95 MB | 335 k | |
| DuckDB → Go | Go `[]struct` | 38 ms | 41 MB ⚠️ | 712 k | pushdown; cgo mem under-counted |
| parquet-go-fast | Go `[]struct` | 129 ms | 102 MB | 334 k | |
| parquet-go | Go `[]struct` | 334 ms | 682 MB | 1.7 M | |
| arrow-go | — | — | — | — | no predicate-pushdown reader |

#### Where we stand on this file

- **We win the deeply-nested full read** — concurrent **77 ms**, ~10× faster than
  parquet-go (single-core 505 ms is still ~1.5× ahead), ~13× DuckDB and ~32× arrow-go.
  Materializing optional structs, lists-of-structs and nested structs is exactly the
  row-path work this library is built for. arrow-go reads all 65 leaf columns into
  columnar Arrow (8.1 GB) without ever building Go rows; DuckDB returns nested fields
  boxed as `map`/`[]interface{}`.
- **Lists need no special tag with this library.** We declare `License []SWLicense`
  / `AdditionalEntities []SWAddEntity` and read them. **parquet-go needs the `,list`
  tag on every list field** (`license,list`, `additional_entities,list`, and the
  inner `aspects,list`/`groups,list`); with bare `[]T` it silently returns empty
  lists — no error. The 770 ms above is the fair run *with* those tags, so both
  readers decode the same data.
- **Scalar projection** drops back to the columnar fast path: fastest, ahead of
  DuckDB and ~8× ahead of parquet-go.
- **The string filter flips with concurrency.** `name < "A"` is a *string* range our
  columnar late-materialization doesn't cover (it's gated to numeric/null-free
  leaves), so single-core we fall to the row path and scan every row → 129 ms, behind
  DuckDB's 38 ms pushdown. But the row-path filter **parallelizes across row groups**:
  `WithConcurrency` brings it to **21 ms — ahead of DuckDB** (which keeps the lower
  memory: 41 MB vs our 95 MB, since it materializes only the matches).

---

**Use parquet-go-fast** when your Go code needs rows as typed structs (ETL, event
replay, feeding services). **Reach for a query engine** (DuckDB, ClickHouse) for
selective analytical queries where you never need the rows materialized in Go.

## Supported types

`inline` = direct typed write · `fast path` = typed when registered, reflect
fallback otherwise · `reflect` = reflect on the hot path.

| Go field type | Path | Notes |
|---|---|---|
| `string`, `bool`, `int`/`int8…64`, `uint8…64`, `float32/64` | inline | narrow ints widen from INT32/INT64 |
| `[]byte` | inline | BYTE_ARRAY |
| `time.Time` | inline | TIMESTAMP (ms/µs/ns) or DATE → UTC instant |
| `*T` (optional scalar, `*[]byte`, `*time.Time`) | inline | `nil` on null/absent |
| `struct{…}` | inline | embedded at the parent offset |
| `*struct{…}` | fast path | `RegisterStructAlloc[T]` |
| `[]T` primitive (`[]int64`, `[]string`, …) | inline | required elements |
| `[]*T` primitive | inline | nullable elements (null → nil, positions kept) |
| `[]Struct` | fast path | `RegisterStructList[T]` |
| `map[K]V` primitive, `map[K]Struct`, `map[K]time.Time`, `map[K1]map[K2]V` | inline / fast path / reflect | K ∈ {string, int32, int64, float64} |

List columns are resolved **structurally** from the schema tree, so a plain `[]T`
field reads any standard 3-level `LIST` regardless of how the repeated/element
levels are named (`element`, `item`, `array` — parquet-cpp, Spark, Arrow, …).
parquet-go's `GenericReader` instead requires an explicit `,list` struct tag to bind
a 3-level list; without it a bare `[]T` maps to a 2-level repeated group and silently
returns empty lists (no error).

**Not supported (errors at `Compile`):** `[]*Struct`, and mixed two-level nesting
without a struct boundary (`map[K][]V`, `[]map[K]V`, `map[K1]map[K2]Struct`). Wrap
the inner collection in a named struct — e.g. use `map[string]struct{ Items []int64 }`
instead of `map[string][]int64`.

## Limitations & safety

- **`unsafe.Pointer`.** A plan stores byte offsets and is bound to a specific Go
  type + schema (the cache key encodes both); `Unmarshal`/`Reader` manage this for
  you. Don't grow the destination during decode — it writes through `&out[i]`.
- **`nil` vs empty.** Parquet doesn't distinguish a `nil` slice/map from an empty
  one for required fields; use optional (`*T` / `,optional`) fields where the
  distinction matters.
- **`time.Time`** decodes to the absolute instant in **UTC**; compare with
  `.Equal`, not `==`/`reflect.DeepEqual`.
- **Concurrency.** A compiled `Plan` is read-only and safe to share. A single
  `Reader` is one consumer (call `Read` from one goroutine; it manages its own
  workers). `WithConcurrency(n>1)` needs a concurrent-safe `io.ReaderAt`.

## Testing & conformance

The suite round-trips every supported shape (`parquet-go` writer → this decoder,
`reflect.DeepEqual` gate) plus production-shaped records, and stream-decodes
multi-row-group files checking order-independent aggregates.

It is also run against the **entire**
[apache/parquet-testing](https://github.com/apache/parquet-testing) corpus — the
Apache spec/compatibility test files written by parquet-mr, parquet-cpp,
parquet-rs, Impala, Spark and Presto, exercising encodings and edge cases our own
writer never emits (DELTA\_\*, BYTE\_STREAM\_SPLIT, RLE\_DICTIONARY, Float16,
INT96, decimals, LZ4/brotli, legacy 2-level lists, maps without required keys,
null pages, …). Every file the reference reader can open decodes with a matching
row count and no panics, and a curated set is compared value-for-value against the
reference reader. (All of the library's own fixtures are synthetic; no customer
data.)

```sh
go test ./...                                                   # full suite
go test -short ./...                                            # skip scale tests (sub-second)
git clone https://github.com/apache/parquet-testing
PARQUET_TESTING_DIR=parquet-testing/data go test -run TestConformance ./...
```

## License

See [LICENSE](LICENSE).
