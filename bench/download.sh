#!/usr/bin/env bash
# Downloads the benchmark parquet files into bench/data/ (gitignored).
#
#   NYC TLC yellow-taxi  — flat numeric, ~48 MB  (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
#   Open-Orca (HF)       — string-heavy,  ~1 GB  (https://huggingface.co/datasets/Open-Orca/OpenOrca)
#
# Usage: ./download.sh [taxi|orca|all]   (default: taxi)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data

fetch() {
	local url="$1" out="$2"
	if [ -f "$out" ]; then
		echo "already present: $out ($(du -h "$out" | cut -f1))"
		return
	fi
	echo "downloading $out ..."
	curl -fSL -o "$out" "$url"
	echo "saved $out ($(du -h "$out" | cut -f1))"
}

what="${1:-taxi}"

if [ "$what" = "taxi" ] || [ "$what" = "all" ]; then
	fetch "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" \
		"data/yellow_tripdata_2024-01.parquet"
fi

if [ "$what" = "orca" ] || [ "$what" = "all" ]; then
	# ~1 GB string-heavy text file.
	fetch "https://huggingface.co/datasets/Open-Orca/OpenOrca/resolve/main/1M-GPT4-Augmented.parquet" \
		"data/openorca.parquet"
fi
