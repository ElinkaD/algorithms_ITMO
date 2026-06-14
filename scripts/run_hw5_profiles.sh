#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_DIR="$ROOT/reports/hw5/profiles"

mkdir -p "$PROFILE_DIR"
rm -f "$PROFILE_DIR"/*.out "$PROFILE_DIR"/*_top.txt

cd "$ROOT/hw5"

echo "[CPU] AND"
go test ./... -run '^$' -count=1 -bench=BenchmarkAndQuery -benchmem \
  -cpuprofile "$PROFILE_DIR/cpu_and.out"

echo "[CPU] OR"
go test ./... -run '^$' -count=1 -bench=BenchmarkOrQuery -benchmem \
  -cpuprofile "$PROFILE_DIR/cpu_or.out"

echo "[CPU] NEAR/3"
go test ./... -run '^$' -count=1 -bench=BenchmarkNearQuery -benchmem \
  -cpuprofile "$PROFILE_DIR/cpu_near.out"

echo "[CPU] BM25"
go test ./... -run '^$' -count=1 -bench=BenchmarkBM25TopK -benchmem \
  -cpuprofile "$PROFILE_DIR/cpu_bm25.out"

echo "[MEM] AND"
go test ./... -run '^$' -count=1 -bench=BenchmarkAndQuery -benchmem \
  -memprofile "$PROFILE_DIR/mem_and.out" -memprofilerate=1

echo "[MEM] OR"
go test ./... -run '^$' -count=1 -bench=BenchmarkOrQuery -benchmem \
  -memprofile "$PROFILE_DIR/mem_or.out" -memprofilerate=1

echo "[MEM] NEAR/3"
go test ./... -run '^$' -count=1 -bench=BenchmarkNearQuery -benchmem \
  -memprofile "$PROFILE_DIR/mem_near.out" -memprofilerate=1

echo "[MEM] BM25"
go test ./... -run '^$' -count=1 -bench=BenchmarkBM25TopK -benchmem \
  -memprofile "$PROFILE_DIR/mem_bm25.out" -memprofilerate=1

cd "$ROOT"

for profile in \
  cpu_and cpu_or cpu_near cpu_bm25 \
  mem_and mem_or mem_near mem_bm25
do
  echo "[TOP] $profile"
  go tool pprof -top "$PROFILE_DIR/$profile.out" > "$PROFILE_DIR/${profile}_top.txt"
done

echo "Saved profiles to $PROFILE_DIR"