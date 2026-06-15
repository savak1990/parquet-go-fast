#!/usr/bin/env python3
"""PyArrow comparison for the NYC-taxi benchmark.

Reports best-of-3 wall time for:
  - columnar read   (pq.read_table)        -> Arrow arrays, no per-row objects
  - row materialize (.to_pylist())         -> Python list of row dicts
  - projected row materialize (5 columns)
  - filtered row materialize (dataset pushdown)

This is a different runtime (CPython) than the Go benchmarks, so the numbers are
not directly comparable to them — but the columnar-vs-rows gap and the filter
pushdown speed are the points of interest. to_pylist() is the row-materialization
equivalent of the Go []struct decode.

Usage: python3 bench/sql/pyarrow_bench.py [path-to-parquet]
Requires: pip install pyarrow
"""
import sys
import time

import pyarrow.parquet as pq
import pyarrow.dataset as ds

F = sys.argv[1] if len(sys.argv) > 1 else "bench/data/yellow_tripdata_2024-01.parquet"
P5 = ["PULocationID", "DOLocationID", "trip_distance", "fare_amount", "total_amount"]


def best(fn, n=3):
    times, result = [], None
    for _ in range(n):
        t = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t)
    return min(times), result


def main():
    print(f"file: {F}")

    t, _ = best(lambda: pq.read_table(F))
    print(f"full   read_table  (columnar)  {t*1000:8.0f} ms")

    t, r = best(lambda: pq.read_table(F).to_pylist())
    print(f"full   to_pylist   (rows)      {t*1000:8.0f} ms  rows={len(r)}")

    t, r = best(lambda: pq.read_table(F, columns=P5).to_pylist())
    print(f"proj5  to_pylist   (rows)      {t*1000:8.0f} ms  rows={len(r)}")

    for name, expr in [("trip_distance>50", ds.field("trip_distance") > 50),
                       ("fare_amount>100", ds.field("fare_amount") > 100)]:
        t, r = best(lambda e=expr: ds.dataset(F).to_table(columns=P5, filter=e).to_pylist())
        print(f"filter {name:16s} (rows)  {t*1000:8.0f} ms  rows={len(r)}")


if __name__ == "__main__":
    main()
