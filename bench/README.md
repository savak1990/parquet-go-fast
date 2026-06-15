# Cross-technology read benchmarks

This directory benchmarks `parquet-go-fast` against other parquet readers on a
real dataset, the way an application actually consumes data: **end to end, from
"start reading" to "I hold a ready-to-use native row collection."**

The headline question is *"where do we stand?"* — answered honestly, including
the cases we lose.

## The one rule: compare within a category

A flat wall-clock comparison across parquet tools is misleading because they
produce **different things**:

| Category | What you get | Tools here |
|---|---|---|
| **A. Row materialization** | `N` native row objects (Go `[]struct`, Python list of dicts) | parquet-go-fast, parquet-go, DuckDB→Go, PyArrow `to_pylist` |
| **B. Columnar decode** | column arrays, *no per-row objects* | arrow-go, PyArrow `read_table` |
| **C. Analytical query** | a scalar / aggregate; rows never leave the engine | DuckDB / ClickHouse `SELECT sum(...)` |

Comparing "build 2.96 M structs" (A) to "compute one `SUM` and discard the data"
(C) is not a benchmark — C does far less work. The Go benchmarks here all target
**category A** (materialize into `[]struct`); arrow-go is reported as **B**
(columnar) since that is its native output; the analytical engine queries in
[`sql/engines.sh`](sql/engines.sh) are **C**, kept only for context and clearly
labeled as not comparable.

## Datasets

Each dataset gets the same three workloads, across different shapes:

- **NYC TLC yellow-taxi** (<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>)
  — `2024-01`, **2,964,624 rows × 19 columns** (int32/int64/timestamp/double/string,
  nullable, dictionary-encoded, zstd, 3 row groups, ~48 MB). Flat numeric — the
  columnar-friendly best case.
- **Open-Orca** ([HF Open-Orca/OpenOrca](https://huggingface.co/datasets/Open-Orca/OpenOrca),
  `1M-GPT4-Augmented`) — **~995 K rows × 4 string columns** (~1 GB). String-heavy —
  exercises the boxed-string fallback and large-value allocation.
- **dbpedia embeddings** ([HF KShivendu/dbpedia-entities-openai-1M](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M))
  — **38,462 rows × 3 strings + a 1536-dim `LIST<double>` embedding** (~350 MB).
  Nested — the list forces the row path; projecting it away returns to columnar.
- **structured-wikipedia** ([HF wikimedia/structured-wikipedia](https://huggingface.co/datasets/wikimedia/structured-wikipedia),
  `enwiki_namespace_0`) — **177,499 rows**, deeply nested (~354 MB): optional structs,
  lists-of-structs, `list<string>`, struct-in-struct. The least parquet-idiomatic
  shape — closest to a real application data model; the full read is all row path.

```sh
./download.sh            # NYC taxi (~48 MB)  → bench/data/yellow_tripdata_2024-01.parquet
./download.sh orca       # Open-Orca (~1 GB)  → bench/data/openorca.parquet
./download.sh dbpedia    # dbpedia (~350 MB)  → bench/data/dbpedia.parquet
./download.sh structwiki # structured-wikipedia (~354 MB) → bench/data/structwiki.parquet
./download.sh all        # everything
```

## Workloads

1. **Full materialization** — all 19 columns → rows.
2. **Projection** — 1 / 5 / 10 of the 19 columns → rows.
3. **Filter** — a predicate → only the matching rows, two selectivities
   (`trip_distance > 50` ≈ 412 rows; `fare_amount > 100` ≈ 7 995 rows).

## Running

### Go (parquet-go-fast vs parquet-go vs DuckDB-Go vs arrow-go)

The Go benchmarks live in [`go/`](go) as a **separate module** (so the heavy
comparison dependencies — the DuckDB cgo driver and arrow-go — never touch the
library's own `go.mod`). It depends on the local library via a `replace`
directive.

```sh
cd go
go test -bench . -benchmem -benchtime=5x -run '^$' -timeout 20m
```

Useful filters:

```sh
go test -bench 'BenchmarkFull_'  -benchmem -benchtime=5x -run '^$'   # workload 1
go test -bench 'BenchmarkProj_'  -benchmem -benchtime=5x -run '^$'   # workload 2 (sub-benchmarks 01col/05col/10col)
go test -bench 'BenchmarkFilter_' -benchmem -benchtime=5x -run '^$'  # workload 3 (sub-benchmarks per predicate)
go test -bench 'Orca'             -benchmem -benchtime=3x -run '^$'  # Open-Orca suite (set ORCA_FILE or ./download.sh orca)
go test -bench 'Wiki'             -benchmem -benchtime=5x -run '^$'  # dbpedia suite (set DBPEDIA_FILE or ./download.sh dbpedia)
go test -bench 'SW'               -benchmem -benchtime=3x -run '^$'  # structured-wikipedia suite (set STRUCTWIKI_FILE or ./download.sh structwiki)
go test -run TestFilterCountDiagnostic -v                            # sanity: pushdown count == engines
```

> **arrow-go caveat.** arrow-go returns *columnar Arrow arrays*, not a Go
> `[]struct`. The `arrow-go → rows` benchmarks are our own transpose on top of it;
> for **string** columns that transpose returns values that alias Arrow's value
> buffer (views), not independent/GC-owned Go strings (they become invalid once
> the Arrow table is released). That's why arrow-go's allocation count on
> string-heavy data is tiny — it doesn't *own* the strings; parquet-go-fast and
> parquet-go produce a `[]struct` whose strings are independent copies.

The first run compiles the DuckDB cgo library (slow); subsequent runs are fast.
`reportRows` adds a `rows/s` metric; filter benchmarks add a `matched` count
instead (output rows/s is meaningless when almost everything is filtered out).

### PyArrow (Python row materialization, separate runtime)

```sh
pip install pyarrow
python3 sql/pyarrow_bench.py
```

`read_table` is columnar (category B); `.to_pylist()` is the row-materialization
equivalent (category A). Different runtime than Go — read it for the
columnar-vs-rows gap and the filter-pushdown speed, not for a head-to-head ns
count against Go.

### DuckDB / ClickHouse analytical (category C, context only)

```sh
./sql/engines.sh     # needs the `duckdb` and `clickhouse` CLIs on PATH
```

## Results — NYC taxi (flat numeric; Apple M4 Pro, Go 1.26, warm cache)

Numbers move with hardware; the **ratios** are the takeaway. All Go rows end in a
`[]struct`; arrow-go's full read is columnar (noted). The **main README's
[Performance](../README.md#performance) section has the full per-reader
Reader / Returns / Time / Mem / Alloc tables** (incl. the DuckDB cgo-memory caveat);
the summaries below are the time/ratio view plus reproduction.

### 1. Full materialization (19 cols → rows)

| Tool | Output | Time | allocs/op |
|---|---|---:|---:|
| **parquet-go-fast (concurrent)** | Go rows | **181 ms** | 1.6 K |
| arrow-go | Arrow (columnar) | 310 ms | 193 K |
| parquet-go-fast (single) | Go rows | 472 ms | 1.7 K |
| DuckDB → Go | Go rows | 2104 ms | 42 M |
| parquet-go `GenericReader` | Go rows | 3089 ms | 24 M |

### 2. Projection (N of 19 cols → rows)

| Columns | parquet-go-fast | arrow-go → rows | DuckDB → Go | parquet-go |
|---|---:|---:|---:|---:|
| 1 | **10 ms** | 17 ms | 136 ms | 1947 ms |
| 5 | **49 ms** | 83 ms | 535 ms | 1997 ms |
| 10 | **99 ms** | 190 ms | 931 ms | 2306 ms |

Concurrent (5 cols): **19 ms**. Allocations stay in the **hundreds** for
parquet-go-fast across all widths (arrow-go: 7.6 K–110 K; DuckDB: 2.9 M–11.7 M;
parquet-go: 17.8 M).

### 3. Filter (predicate → matching rows)

| Predicate (matches) | parquet-go-fast | …concurrent | DuckDB → Go | PyArrow | parquet-go |
|---|---:|---:|---:|---:|---:|
| `trip_distance > 50` (412) | 43 ms | **16 ms** | 8.8 ms | ~9 ms | 1995 ms |
| `fare_amount > 100` (7 995) | 43 ms | 17 ms | **12 ms** | ~14 ms | 1986 ms |

(PyArrow row count materialized via dataset pushdown + `to_pylist`; arrow-go has no
predicate-pushdown reader. DuckDB filter Go-heap mem is tiny — 47 KB / 1.2 MB — since
it materializes only the matches.)

## How to read it

- **Full materialization → Go structs: parquet-go-fast wins.** The concurrent
  read is ~1.7× faster than arrow-go's columnar read while returning usable
  structs, with ~4 orders of magnitude fewer allocations than the engine paths.
- **Projection → Go structs: parquet-go-fast leads at every width** (~1.7–1.8×
  faster than arrow-go→rows; 4–10× faster than DuckDB→Go). The Tier 2 columnar
  decode reads numeric columns straight from the page's typed buffer (a typed
  dictionary gather, no `parquet.Value` boxing), and writes directly into structs
  — arrow-go pays an extra columnar-arrays→structs transpose we don't.
- **Selective filter → rows: competitive.** The filter decodes the output columns
  once (typed), evaluates the predicate over the decoded values, and keeps the
  matches — ~45× faster than parquet-go. Single-core we trail DuckDB by ~3.5×, and
  **~1.4× with `WithConcurrency`** on the 7 995-match case (it still wins via SIMD
  predicate eval and by materializing only the matches — note its tiny Go-heap mem).
  Heavily-pruned scans on sorted/clustered columns use the row+seek path and read
  only a tiny fraction of the file. (On other datasets concurrency flips this — see
  dbpedia and structured-wikipedia below, where concurrent filter beats DuckDB.)

**Net:** for materializing parquet into Go row structs we are the fastest measured
option — at full reads *and* projection, ahead of even arrow-go's columnar reader;
for selective analytical queries, purpose-built engines are the right tool and we
don't pretend to compete.

## Results — Open-Orca (string-heavy; `-benchtime=3x`)

~995 K rows × 4 string columns (~1 GB). Strings can't use the typed numeric gather
(boxed columnar fallback), and decode is dominated by copying bytes into Go
strings — so this is a *competitive*, not dominant, case.

| Workload | parquet-go-fast | arrow-go → rows | DuckDB → Go | parquet-go |
|---|---:|---:|---:|---:|
| Full (4 cols) | 1645 / **1618** (conc) ms | 2289 ms | 2187 ms | 2300 ms |
| Projection (2 cols) | **962 ms** | 1011 ms | 1001 ms | 1154 ms |
| Filter (`system_prompt==…`, 224 K) | 920 ms | — | **544 ms** | 2167 ms |

allocs/op, full read: parquet-go-fast 3.9 M · arrow-go **27 K** · parquet-go 6.9 M
· DuckDB 16 M. Memory, full read: parquet-go-fast **2.0 GB** · DuckDB 4.2 GB ·
arrow-go 5.8 GB · parquet-go 4.2 GB.

- We're **~tied with arrow-go** and modestly ahead of parquet-go/DuckDB on full +
  projection — the numeric blowout doesn't carry over, because string decode is
  byte-copy-bound (every reader pays) and can't use the typed gather.
- **arrow-go is far leaner on allocations** (27 K vs 3.9 M): it keeps strings in one
  buffer (views), while we copy each value into an independent, GC-safe Go string
  (~1 alloc/value). A real trade-off.
- The **string filter takes the row path** (string predicate ≠ numeric columnar
  filter); ~1.7× behind DuckDB. **Concurrency barely helps** (alloc/GC-bound).

## Results — dbpedia embeddings (nested list; `-benchtime=5x`)

38,462 rows × 3 strings + a 1536-dim `LIST<double>` embedding. The list forces the
row path on the full read; the projection drops it (columnar path).

| Workload | parquet-go-fast | arrow-go → rows | DuckDB → Go | parquet-go |
|---|---:|---:|---:|---:|
| Full (incl. embedding) | 642 / **73** (conc) ms | 741 ms | 636 ms (boxed list) | 1386 ms (needs `,list`) |
| Projection (id+title) | 3.2 / **0.8** (conc) ms | 3.2 ms | 12 ms | 7.8 ms |
| Filter (`title<"M"`, 22 634) | 4.5 / **0.7** (conc) ms | — | 6.9 ms | 8.1 ms |

- **We win the nested full read** (concurrent 73 ms, ~9× faster than DuckDB and
  arrow-go, ~19× parquet-go) — building Go `[]float64` slices directly beats
  arrow-go's columnar→row transpose and DuckDB's per-element `[]interface{}` boxing
  (60 M allocs). Memory, full read: parquet-go-fast **678 MB** · DuckDB 1.6 GB ·
  arrow-go 2.7 GB · parquet-go 4.8 GB.
- **Lists need no special tag here; parquet-go requires `,list`.** Our reader takes a
  plain `[]float64`. parquet-go binds a 3-level `LIST` only with the `openai,list`
  tag — a bare `[]float64` silently reads empty (no error). The 1386 ms above is the
  *fair* run with the tag set, where parquet-go actually decodes the embedding (4.8 GB
  allocations). The element name (`item` vs the spec-default `element`) is irrelevant
  — parquet-go matches list elements structurally once the field is tagged.
- **DuckDB returns the embedding boxed** as `[]interface{}` of `float64` (kept as-is,
  not converted to `[]float64`). Projection and the string filter both go our way —
  concurrent sub-millisecond, ahead of every engine.

## Results — structured-wikipedia (deeply nested; `-benchtime=3x`)

177,499 enwiki rows, 19 top-level fields → 65 leaf columns: optional structs,
lists-of-structs, `list<string>`, struct-in-struct. The full read is all row path;
the scalar projection drops back to columnar; the *string* filter takes the row path
(no columnar late-mat for strings) but parallelizes. arrow-go reads the full nested
table columnar (no row transpose); DuckDB returns nested fields boxed.

| Workload | parquet-go-fast | DuckDB → Go | arrow-go | parquet-go |
|---|---:|---:|---:|---:|
| Full (every nested field) | 505 / **77** (conc) ms | 985 ms (boxed) | 2473 ms (columnar) | 770 ms (needs `,list`) |
| Projection (id+name+url) | 43 / **11** (conc) ms | 63 ms | — (bespoke) | 334 ms |
| Filter (`name<"A"`, 79 003) | 129 / **21** (conc) ms | 38 ms | — (no pushdown) | 334 ms |

- **We win the deeply-nested full read** (concurrent 77 ms, ~10× faster than
  parquet-go, ~13× DuckDB, ~32× arrow-go; single-core 505 ms still ~1.5× parquet-go)
  — materializing optional/nested structs and lists-of-structs is the row-path work
  this library exists for. arrow-go reads all 65 leaf columns into columnar Arrow
  (8.1 GB, no row build); DuckDB boxes nested fields as `map`/`[]interface{}`.
- **Lists need no special tag here; parquet-go requires `,list`.** Our reader takes
  plain `[]SWLicense` / `[]SWAddEntity`. parquet-go needs `,list` on every list field
  (`license,list`, `additional_entities,list`, inner `aspects,list`/`groups,list`) or
  it silently returns empty lists. The 770 ms above is the fair run with all those
  tags set (`ArticlePG`), so both readers decode the same data.
- **Scalar projection** returns to the columnar path: fastest, ~6× ahead of DuckDB,
  ~8× ahead of parquet-go.
- **The string filter flips with concurrency.** `name < "A"` is a string range our
  columnar late-materialization doesn't cover (gated to numeric/null-free leaves), so
  single-core we scan every row → 129 ms, behind DuckDB's 38 ms pushdown. But the
  row-path filter parallelizes across row groups: `WithConcurrency` → **21 ms, ahead
  of DuckDB** (which keeps lower memory, 41 MB vs 95 MB, materializing only matches).

## Fairness notes / caveats

- **Warm cache.** The Go decoders read an in-memory copy; DuckDB reads the file
  (OS page cache, warm after the first read). Both are decode-bound, not I/O.
- **Output shape is stated per row.** Comparing across categories (A/B/C) is only
  done with the shape called out — never silently.
- **Threads.** parquet-go-fast "concurrent" uses `GOMAXPROCS`; "single" is one
  goroutine. DuckDB parallelizes its scan, but its `database/sql` `Scan` into Go
  (the row materialization) is single-threaded and dominates its time — those
  42 M allocs are the driver boxing every cell.
- **PyArrow is CPython** — a different runtime; treat its numbers as
  cross-ecosystem context, not a same-VM head-to-head.
- **Selectivity matters.** These predicates scatter across the file. A predicate
  aligned with row-group/page ordering (e.g. a range on a clustered/sorted
  column) lets parquet-go-fast's pruning skip most of the file — a case where the
  filter gap narrows sharply.
