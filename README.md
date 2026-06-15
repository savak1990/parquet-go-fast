# parquet-go-fast

A high-performance, reflection-free-on-the-hot-path **parquet decoder** for Go. It
reads parquet files into Go structs far faster — and with orders of magnitude
fewer allocations — than the reflection-driven reader in
[`parquet-go/parquet-go`](https://github.com/parquet-go/parquet-go).

```go
import parquetfast "github.com/savak1990/parquet-go-fast"

rows, err := parquetfast.UnmarshalFile[MyRow]("data.parquet") // []MyRow
```

It is **decode-only**, depends only on `parquet-go` (no fork, no `replace`), and
reads files written by any spec-conformant writer (parquet-go, Arrow, Spark,
DuckDB, pandas/pyarrow, …). It compiles the `(Go type, file schema)` mapping
**once** and then decodes every row through precompiled, typed `unsafe.Pointer`
writes — no per-row reflection.

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
| PyArrow | Python [`pyarrow`](https://arrow.apache.org/docs/python/) — `read_table` (columnar) / `to_pylist` (rows) | Arrow / Python list |

\* Only parquet-go-fast and parquet-go return a Go `[]struct` natively. **arrow-go
returns columnar Arrow arrays** — the "→ rows" numbers are *our* transpose on top,
and for string columns those values alias Arrow's buffer (views), not independent
Go strings (which is why its allocation counts look so low). **DuckDB → Go** is the
real in-process cgo driver going through `database/sql` (the per-cell `Scan` is the
bulk of its allocations) — not the CLI. **PyArrow** is a different runtime (Python),
shown for cross-ecosystem context. The DuckDB/ClickHouse *CLI* appears only in
[`bench/sql/engines.sh`](bench/sql/engines.sh) for scalar analytical queries that
never materialize rows — not in any table here.

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

**Full read — all 19 columns → rows.** Read every row and column into records.
The Go readers and DuckDB→Go return a `[]struct`; arrow-go returns columnar Arrow
arrays (no per-row objects, so it does strictly less work), shown for reference.

| Reader | Returns | Time |
|---|---|---:|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **173 ms** |
| arrow-go | Arrow columns | 311 ms |
| parquet-go-fast (single core) | Go `[]struct` | 440 ms |
| DuckDB → Go | Go `[]struct` | 2103 ms |
| parquet-go | Go `[]struct` | 3041 ms |

**Projection — N of 19 columns → rows.** Read only N columns (a struct with just
those fields) — tests how well a reader avoids touching the rest. `arrow-go → rows`
reads N columns into Arrow then transposes to structs (that transpose is in its time).

| Columns | parquet-go-fast | arrow-go → rows | DuckDB → Go | parquet-go |
|---|---:|---:|---:|---:|
| 1 | **10 ms** | 17 ms | 135 ms | 1905 ms |
| 5 | **48 ms** | 82 ms | 533 ms | 2046 ms |
| 10 | **99 ms** | 183 ms | 937 ms | 2292 ms |

**Filter — predicate → matching rows.** Apply a `WHERE` and return only the matches.
parquet-go has no pushdown (decode all rows, filter in Go); DuckDB and PyArrow push
the predicate down. arrow-go has no pushdown reader, so it isn't listed here.
parquet-go-fast is shown single-core and with `WithConcurrency`.

| Predicate (matches) | parquet-go-fast | …concurrent | DuckDB → Go | PyArrow | parquet-go |
|---|---:|---:|---:|---:|---:|
| `trip_distance > 50` (412) | 44 ms | **17 ms** | 9 ms | ~9 ms | 1971 ms |
| `fare_amount > 100` (7 995) | 43 ms | **17 ms** | 12 ms | ~14 ms | 1968 ms |

#### Where we stand on this file

- ✅ **Fastest way to get Go structs out of parquet.** We win full reads — 1.7×
  faster than arrow-go's columnar read, which doesn't even build structs — and
  projection at every width (~1.7–1.8× faster than arrow-go→rows, 4–10× faster
  than DuckDB→Go), with allocations in the **hundreds** vs thousands–millions.
- 🟡 **Selective filters are competitive, no longer a blowout.** A filtered read
  decodes the output columns once (typed), evaluates the predicate over the
  decoded values, and keeps the matches — ~10× faster than the old per-row path
  and **~40× faster than parquet-go**. We trail DuckDB/PyArrow by ~5× single-core
  and **~2× with `WithConcurrency`** (they still win via SIMD predicate eval and
  by skipping output decode for non-matching pages). Row-group/page pruning makes
  selective scans on sorted/clustered columns far cheaper still.

### Open-Orca — string-heavy text (HuggingFace)

[Open-Orca/OpenOrca](https://huggingface.co/datasets/Open-Orca/OpenOrca),
~995 K rows × **4 string columns** (~1 GB). The opposite of taxi: all `BYTE_ARRAY`,
so the typed numeric gather doesn't apply (strings use the boxed columnar
fallback) and decode is dominated by copying bytes into Go strings. (`-benchtime=3x`.)

The decoded record — all strings:

```go
type OpenOrca struct {
    ID           string `parquet:"id"`
    SystemPrompt string `parquet:"system_prompt"`
    Question     string `parquet:"question"`
    Response     string `parquet:"response"`
}
```

**Full read — 4 columns → rows**

| Reader | Time | allocs/op |
|---|---:|---:|
| **parquet-go-fast** (concurrent) | **1562 ms** | 3.9 M |
| arrow-go → rows | 1710 ms | 27 K |
| parquet-go-fast (single core) | 1784 ms | 3.9 M |
| parquet-go | 1928 ms | 6.9 M |
| DuckDB → Go | 2076 ms | 15.7 M |

**Projection — 2 of 4 → rows**

| Reader | Time |
|---|---:|
| arrow-go → rows | 913 ms |
| **parquet-go-fast** | 920 ms |
| DuckDB → Go | 967 ms |
| parquet-go | 1062 ms |

**Filter — `system_prompt == …` (224 K matches) → rows**

| Reader | Time |
|---|---:|
| **DuckDB → Go** | 531 ms |
| **parquet-go-fast** | 886 ms |
| parquet-go | 1849 ms |

#### Where we stand on this file

- **Competitive, not dominant.** On strings we're ~tied with arrow-go and modestly
  ahead of parquet-go/DuckDB — the numeric blowout doesn't carry over, because
  string decode is **byte-copy-bound** (every reader pays it) and can't use the
  typed gather.
- **arrow-go doesn't actually hand back a Go slice here.** It returns columnar
  Arrow arrays; the `arrow-go → rows` column is *our* transpose, and for strings it
  yields values that **alias Arrow's buffer (views)** — not independent Go strings,
  and invalid once the Arrow table is released. That's why its alloc count is 27 K
  vs our 3.9 M: it never *owns* the strings, whereas a `[]OpenOrca` from
  parquet-go-fast (or parquet-go) is fully independent, GC-safe data. Apples to
  oranges on allocations — read the *time* as the comparable figure.
- **String filters take the row path** (string predicate ≠ the numeric columnar
  filter); ~1.7× behind DuckDB — far closer than numeric filters once were.
- **Concurrency barely helps here** — string materialization is allocation/GC-bound,
  not CPU-bound.

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

| Reader | Time | Note |
|---|---:|---|
| **parquet-go-fast** (concurrent) | **73 ms** | ✅ |
| parquet-go-fast (single core) | 619 ms | ✅ |
| DuckDB → Go | 673 ms | ✅ (list → `[]interface{}` conversion) |
| arrow-go → rows | 751 ms | ✅ (columnar → row transpose) |
| parquet-go | ~23 ms | ⚠️ **drops the embedding (empty `[]float64`)** |

**Projection — id + title (no embedding → columnar) → rows**

| Reader | Time |
|---|---:|
| arrow-go → rows | 3.1 ms |
| **parquet-go-fast** | 3.1 ms |
| parquet-go | 7.7 ms |
| DuckDB → Go | 11.7 ms |

**Filter — `title < "M"` (22 634 matches) → rows**

| Reader | Time |
|---|---:|
| **parquet-go-fast** | 4.4 ms |
| DuckDB → Go | 6.6 ms |
| parquet-go | 7.8 ms |

#### Where we stand on this file

- **We win the nested full read** — concurrent **73 ms**, ~9–10× faster than
  DuckDB and arrow-go. The row path isn't a liability here: building Go `[]float64`
  slices *directly* beats arrow-go's columnar→row transpose and DuckDB's
  per-element `[]interface{}` boxing through `database/sql`.
- ⚠️ **parquet-go silently drops the embedding** (`len == 0`). The list element is
  named `item`; parquet-go's `GenericReader` assumes the spec-default `element` and
  returns empty lists. We resolve the element **structurally** and read it
  correctly — a real correctness win on real-world data (its ~23 ms is decoding
  nothing for that column).
- **Projection that drops the list** is scalar-only → the columnar fast path: tied
  with arrow-go, ahead of the engines.
- **String-range filter:** we're the fastest here too.

---

### structured-wikipedia — deeply nested, application-shaped (HuggingFace)

[wikimedia/structured-wikipedia](https://huggingface.co/datasets/wikimedia/structured-wikipedia),
177,499 rows (~354 MB). The **least** parquet-idiomatic file here and the closest to
a real application data model: optional structs, lists-of-structs, a `list<string>`,
and a struct-nested-in-a-struct. Nothing about this is columnar-friendly — the full
read is *all* row path. (arrow-go/PyArrow are omitted: a hand-written nested→Go
transpose for a schema this deep is bespoke and fragile, not a fair apples-to-apples
column read.)

```go
type Article struct {
    Identifier  int64     `parquet:"identifier"`
    Name        string    `parquet:"name"`
    Abstract    string    `parquet:"abstract"`
    URL         string    `parquet:"url"`
    DateCreated time.Time `parquet:"date_created"`

    Image      *SWImage  `parquet:"image"`       // optional struct
    MainEntity *SWEntity `parquet:"main_entity"` // optional struct

    AdditionalEntities []SWAddEntity `parquet:"additional_entities"` // list<struct{ list<string>, … }>
    License            []SWLicense   `parquet:"license"`             // list<struct>

    Version *SWVersion `parquet:"version"` // struct → *SWEditor (struct-in-struct)
}
```

**Full read — every nested field → rows**

| Reader | Time | Note |
|---|---:|---|
| **parquet-go-fast** (concurrent) | **77 ms** | ✅ |
| parquet-go-fast (single core) | 490 ms | ✅ |
| parquet-go | 597 ms | ⚠️ **drops `license` + `additional_entities` (empty lists)** |

**Projection — `identifier` + `name` + `url` (scalar → columnar) → rows**

| Reader | Time |
|---|---:|
| **parquet-go-fast** | 41 ms |
| DuckDB → Go | 62 ms |
| parquet-go | 320 ms |

**Filter — `name < "A"` (79 003 matches) → rows**

| Reader | Time |
|---|---:|
| **DuckDB → Go** | 35 ms |
| parquet-go-fast | 126 ms |
| parquet-go | 320 ms |

#### Where we stand on this file

- **We win the deeply-nested full read** — concurrent **77 ms**, ~8× faster than
  parquet-go. Materializing optional structs, lists-of-structs and nested structs
  is exactly the row-path work this library is built for.
- ⚠️ **parquet-go silently drops two list-of-struct fields** (`license`,
  `additional_entities`) — same root cause as dbpedia: their list elements aren't
  named the spec-default `element`, so `GenericReader` returns empty. We resolve
  them structurally and decode them.
- **Scalar projection** drops back to the columnar fast path: fastest, ahead of
  DuckDB and ~8× ahead of parquet-go.
- **Selective string filter is our weak spot, and we show it.** `name < "A"` is a
  *string* range predicate, which our columnar late-materialization doesn't cover
  (it's gated to numeric/null-free leaves), so we fall back to the row path and scan
  every row → **126 ms**. DuckDB prunes row groups, stays columnar, and materializes
  only the 79 k matches → **35 ms**. We still beat parquet-go ~2.5×, but here a query
  engine is the right tool — exactly the boundary called out below.

---

**Use parquet-go-fast** when your Go code needs rows as typed structs (ETL, event
replay, feeding services). **Reach for a query engine** (DuckDB, ClickHouse) for
selective analytical queries where you never need the rows materialized in Go.

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

Reuse a destination buffer instead of holding the whole `[]T`:

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

Decoding is split into two stages so the per-row work carries no reflection.

**Stage 1 — Plan (once per `(Go type, schema)`, cached).** Reflection walks the
struct and the parquet schema a single time and emits a flat list of scalar
setters — `{column index, field byte-offset, kind}` — plus a closure per compound
field (map, list, optional struct). Plans are cached process-wide, keyed on the
type, schema, and null-column shape.

**Stage 2 — Apply (per row, no reflection).** Each value is written straight to
`base + offset` through `unsafe.Pointer` and a typed enum-`switch` — no
`reflect.Value`, no interface dispatch, no per-leaf closure call.

**Columnar fast path.** For **scalar-only** schemas (no maps/lists/optional
structs) the decoder skips parquet-go's row reader entirely and reads
**column-at-a-time**, writing strided into the destination structs. Numeric
columns decode **straight from the page's typed buffer** — resolving dictionary
indices against the dictionary's typed values and placing nulls from the
definition levels, with no `parquet.Value` boxing and one typed store per cell.
That is what makes full reads and projection beat even arrow-go's columnar path.
Strings/bools/`time.Time`/optionals fall back to a (still columnar) boxed read;
any compound field falls back to the row path, so nested schemas are unaffected.
The whole path composes with concurrency (each worker owns a disjoint output
region).

On top of the hot path: a process-wide plan cache, all-null-column elision,
column projection, and predicate pushdown (row-group + page pruning, sorted-column
binary search, bloom filters). Filtered reads of scalar-only structs use the same
columnar decode — output columns are decoded once, the predicate is evaluated over
the decoded values, and only matches are kept — and parallelize across row groups
with `WithConcurrency`; heavily-pruned scans (sorted columns) fall back to a
row-at-a-time path that seeks over skipped pages.

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

The list element is resolved structurally, so files whose element is named `item`
or `array` (parquet-cpp, some Spark/Presto output) decode correctly — where
parquet-go's `GenericReader` silently returns empty lists.

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
