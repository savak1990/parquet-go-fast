#!/usr/bin/env bash
# Downloads the benchmark parquet files into bench/data/ (gitignored).
#
#   NYC TLC yellow-taxi  — flat numeric,        ~48 MB  (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
#   Open-Orca (HF)       — string-heavy,         ~1 GB   (https://huggingface.co/datasets/Open-Orca/OpenOrca)
#   dbpedia (HF)         — nested list + strings, ~350 MB (https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)
#
# Usage: ./download.sh [taxi|orca|dbpedia|all]   (default: taxi)
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

if [ "$what" = "dbpedia" ] || [ "$what" = "all" ]; then
	# ~350 MB; 3 strings + a 1536-dim LIST<double> embedding (nested).
	fetch "https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet" \
		"data/dbpedia.parquet"
fi
