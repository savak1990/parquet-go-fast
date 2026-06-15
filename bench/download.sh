#!/usr/bin/env bash
# Downloads the NYC TLC yellow-taxi benchmark file into bench/data/ (gitignored).
# Public dataset: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p data
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
OUT="data/yellow_tripdata_2024-01.parquet"

if [ -f "$OUT" ]; then
	echo "already present: $OUT ($(du -h "$OUT" | cut -f1))"
	exit 0
fi

echo "downloading $URL ..."
curl -fSL -o "$OUT" "$URL"
echo "saved $OUT ($(du -h "$OUT" | cut -f1))"
