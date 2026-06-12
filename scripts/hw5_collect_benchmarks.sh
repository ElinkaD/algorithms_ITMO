#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW="$ROOT/reports/hw5/results/go_bench_raw.txt"
OUT="$ROOT/reports/hw5/results/go_bench.tsv"

if [[ ! -f "$RAW" ]]; then
  echo "missing $RAW; run scripts/hw5_run_benchmarks.sh first" >&2
  exit 1
fi

printf "benchmark\tns_per_op\tbytes_per_op\tallocs_per_op\n" > "$OUT"
awk '
  /^Benchmark/ {
    name=$1
    ns=""
    bytes=""
    allocs=""
    for (i=1; i<=NF; i++) {
      if ($(i+1) == "ns/op") ns=$i
      if ($(i+1) == "B/op") bytes=$i
      if ($(i+1) == "allocs/op") allocs=$i
    }
    print name "\t" ns "\t" bytes "\t" allocs
  }
' "$RAW" >> "$OUT"

cp "$OUT" "$ROOT/reports/hw5/results/query_latency.tsv"
echo "written $OUT"
