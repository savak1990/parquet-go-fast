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

Measured on the real
[NYC TLC yellow-taxi file](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
(2.96 M rows × 19 mixed columns; warm cache; Apple M4 Pro, Go 1.26), against the
other Go readers and query engines — each materializing into a **ready-to-use
native row collection**. Reproduce and read the methodology in [`bench/`](bench/).

> **What each reader hands back matters.** Only parquet-go-fast and parquet-go
> return an idiomatic Go `[]struct` directly. **arrow-go** returns *columnar Arrow
> arrays* — its "→ rows" numbers below include the column→struct transpose you'd
> otherwise write yourself. **DuckDB→Go** goes through `database/sql` (per-cell
> `Scan`); **PyArrow** is a different runtime (Python objects).

**Full read — all 19 columns → rows.** Read every row and column into records.
The Go readers and DuckDB→Go return a `[]struct`; arrow-go returns columnar Arrow
arrays (no per-row objects, so it does strictly less work), shown for reference.

| Reader | Returns | Time |
|---|---|---:|
| **parquet-go-fast** (concurrent) | Go `[]struct` | **182 ms** |
| arrow-go | Arrow columns | 309 ms |
| parquet-go-fast (single core) | Go `[]struct` | 482 ms |
| DuckDB → Go | Go `[]struct` | 2138 ms |
| parquet-go | Go `[]struct` | 3061 ms |

**Projection — N of 19 columns → rows.** Read only N columns (a struct with just
those fields) — tests how well a reader avoids touching the rest. `arrow-go → rows`
reads N columns into Arrow then transposes to structs (that transpose is in its time).

| Columns | parquet-go-fast | arrow-go → rows | DuckDB → Go | parquet-go |
|---|---:|---:|---:|---:|
| 1 | **11 ms** | 17 ms | 138 ms | 1790 ms |
| 5 | **49 ms** | 82 ms | 501 ms | 1968 ms |
| 10 | **98 ms** | 186 ms | 925 ms | 2205 ms |

**Filter — predicate → matching rows.** Apply a `WHERE` and return only the matches.
parquet-go has no pushdown (decode all rows, filter in Go); DuckDB and PyArrow push
the predicate down. arrow-go has no pushdown reader, so it isn't listed here.

| Predicate (matches) | parquet-go-fast | DuckDB → Go | PyArrow | parquet-go |
|---|---:|---:|---:|---:|
| `trip_distance > 50` (412) | 463 ms | **9 ms** | ~9 ms | 1890 ms |
| `fare_amount > 100` (7 995) | 464 ms | **12 ms** | ~14 ms | 1913 ms |

### Where we stand — honestly

- ✅ **Fastest way to get Go structs out of parquet.** We win full reads — 1.7×
  faster than arrow-go's columnar read, which doesn't even build structs — and
  projection at every width (~1.7–1.8× faster than arrow-go→rows, 4–10× faster
  than DuckDB→Go), with allocations in the **hundreds** vs thousands–millions.
- ❌ **We lose selective filters to query engines by ~50×.** DuckDB/PyArrow decode
  only the filter column, then fetch other columns *only for matching rows* (late
  materialization) and evaluate the predicate with SIMD. We currently decode all
  selected columns and filter per row. We stay ~4× ahead of the other Go reader;
  closing the engine gap (late materialization) is planned.

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
binary search, bloom filters).

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
