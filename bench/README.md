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

## Dataset

NYC TLC **yellow-taxi** trip records (public:
<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>) — `2024-01`,
**2,964,624 rows × 19 columns** (int32/int64/timestamp/double/string, all
nullable, dictionary-encoded, zstd-compressed, 3 row groups, ~48 MB on disk).

```sh
./download.sh        # → bench/data/yellow_tripdata_2024-01.parquet  (gitignored)
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
go test -run TestFilterCountDiagnostic -v                            # sanity: pushdown count == engines
```

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

## Results (Apple M4 Pro, Go 1.26, parquet-go v0.30.1, warm cache)

Numbers move with hardware; the **ratios** are the takeaway. All Go rows end in a
`[]struct`; arrow-go's full read is columnar (noted).

### 1. Full materialization (19 cols → rows)

| Tool | Output | Time | allocs/op |
|---|---|---:|---:|
| **parquet-go-fast (concurrent)** | Go rows | **269 ms** | 1.8 K |
| arrow-go | Arrow (columnar) | 308 ms | 193 K |
| parquet-go-fast (single) | Go rows | 740 ms | 1.8 K |
| DuckDB → Go | Go rows | 2103 ms | 42 M |
| parquet-go `GenericReader` | Go rows | 3048 ms | 24 M |

### 2. Projection (N of 19 cols → rows)

| Columns | parquet-go-fast | arrow-go → rows | DuckDB → Go | parquet-go |
|---|---:|---:|---:|---:|
| 1 | 24 ms | **17 ms** | 133 ms | 1896 ms |
| 5 | 121 ms | **82 ms** | 498 ms | 2010 ms |
| 10 | 239 ms | **184 ms** | 922 ms | 2290 ms |

Allocations stay in the **hundreds** for parquet-go-fast across all widths
(arrow-go: 7.5 K–110 K; DuckDB: 2.9 M–11.6 M; parquet-go: 17.8 M).

### 3. Filter (predicate → matching rows)

| Predicate (matches) | parquet-go-fast | DuckDB → Go | PyArrow | parquet-go |
|---|---:|---:|---:|---:|
| `trip_distance > 50` (412) | 465 ms | **9 ms** | ~8 ms | 2002 ms |
| `fare_amount > 100` (7 995) | 463 ms | **12 ms** | — | 2005 ms |

(PyArrow row count materialized via dataset pushdown + `to_pylist`.)

## How to read it

- **Full materialization → Go structs: parquet-go-fast wins.** The concurrent
  read even beats arrow-go's columnar read while returning usable structs, and
  it does so with ~4 orders of magnitude fewer allocations than the engine paths.
- **Projection → Go structs: arrow-go leads by ~1.3–1.5×.** Its columnar reader
  avoids the `parquet.Value` boxing we still pay; we're a clear second and
  4–5× ahead of DuckDB→Go. (Eliminating that boxing — a typed dictionary gather
  — is the planned "Tier 2" optimization.)
- **Selective filter → rows: the engines win by ~50×, and we say so.** DuckDB and
  PyArrow decode only the filter column, locate matches, then fetch other columns
  *only for matched rows* (late materialization). We prune row groups/pages by
  statistics, but scattered predicates prune little, so we still scan the filter
  column for every row (≈ full-projection cost). We remain ~4× ahead of the only
  other Go option. Closing this needs late materialization (not yet implemented).

**Net:** for materializing parquet into Go row structs we are the fastest measured
option (full) or a close second (projection); for selective analytical queries,
purpose-built engines are the right tool and we don't pretend to compete.

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
