#!/usr/bin/env bash
# Analytical (scalar-returning) queries on the NYC-taxi file via the DuckDB and
# ClickHouse CLIs, for context only.
#
# IMPORTANT: these return an aggregate / count and NEVER materialize rows to the
# client, so they are NOT comparable to the row-materialization benchmarks in
# ../go (which build a full []struct). They show what the engines are built for —
# prune + vectorize + return a scalar — and why that is a different category of
# work. The fair "engine -> Go rows" comparison lives in ../go (DuckDB Go driver).
set -euo pipefail
cd "$(dirname "$0")/.."
F="data/yellow_tripdata_2024-01.parquet"

echo "== duckdb (analytical) =="
printf ".timer on
SELECT count(*) AS rows FROM '%s';
SELECT sum(total_amount), avg(trip_distance) FROM '%s';
SELECT count(*) FROM '%s' WHERE trip_distance > 50;
" "$F" "$F" "$F" | duckdb

echo "== clickhouse (analytical) =="
clickhouse local --time --query "SELECT count(*) FROM file('$F','Parquet') WHERE trip_distance > 50"
clickhouse local --time --query "SELECT sum(total_amount), avg(trip_distance) FROM file('$F','Parquet')"
