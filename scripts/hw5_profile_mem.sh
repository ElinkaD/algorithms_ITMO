#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HW5_DIR="$ROOT/hw5"
PROFILE_DIR="$ROOT/reports/hw5/profiles/mem"

mkdir -p "$PROFILE_DIR"
rm -f "$PROFILE_DIR"/*.out "$PROFILE_DIR"/*_top.txt

cd "$HW5_DIR"

BENCHMARKS=(
  BenchmarkAndQuery
  BenchmarkOrQuery
  BenchmarkNotQuery
  BenchmarkAdjQuery
  BenchmarkNearQuery
  BenchmarkBM25TopK
  BenchmarkWikiAndQuery
  BenchmarkWikiOrQuery
  BenchmarkWikiNotQuery
  BenchmarkWikiAdjQuery
  BenchmarkWikiNearQuery
  BenchmarkWikiPhraseQuery
  BenchmarkWikiComplexQuery
  BenchmarkWikiBM25TopK
  BenchmarkWikiTFIDFTopK
  BenchmarkWikiMmapLookup
  BenchmarkWikiMemoryVsMmapAnd
)

for bench in "${BENCHMARKS[@]}"; do
  out_name="$(echo "$bench" | sed 's/^Benchmark//' | tr '[:upper:]' '[:lower:]')"

  echo "[MEM] $bench"

  go test . \
    -run '^$' \
    -count=1 \
    -bench "^${bench}$" \
    -benchmem \
    -memprofile "$PROFILE_DIR/${out_name}.out" \
    -memprofilerate=1

  go tool pprof -top "$PROFILE_DIR/${out_name}.out" \
    > "$PROFILE_DIR/${out_name}_top.txt"
done

echo "[OK] Memory profiles saved to $PROFILE_DIR"