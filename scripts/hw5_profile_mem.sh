#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HW5_DIR="$ROOT/hw5"
PROFILE_DIR="$ROOT/reports/hw5/profiles/mem"
BIN_DIR="$ROOT/reports/hw5/profiles/bin"
APP_BIN="$BIN_DIR/searchdemo"

GOCACHE="${GOCACHE:-/tmp/go-build-hw5}"
GOMODCACHE="${GOMODCACHE:-/tmp/go-mod-cache-hw5}"
WIKI="${WIKI:-./data/wiki_sample.jsonl}"
DOCS="${DOCS:-50000}"
INDEX="${INDEX:-./data/wiki_50k.seg}"
MEM_ITERATIONS="${MEM_ITERATIONS:-50}"

mkdir -p "$PROFILE_DIR"
mkdir -p "$BIN_DIR"
rm -f "$PROFILE_DIR"/clean_*.out "$PROFILE_DIR"/clean_*_top.txt

cd "$HW5_DIR"

env GOCACHE="$GOCACHE" GOMODCACHE="$GOMODCACHE" \
  go build -o "$APP_BIN" ./cmd/searchdemo

SCENARIOS=(
  wiki-near-query
  wiki-complex-query
  wiki-bm25-topk
  wiki-mmap-materialize-near
  wiki-mmap-lookup
)

for scenario in "${SCENARIOS[@]}"; do
  out_path="$PROFILE_DIR/clean_${scenario}.out"
  echo "[MEM] $scenario"

  env GOCACHE="$GOCACHE" GOMODCACHE="$GOMODCACHE" \
    "$APP_BIN" \
    --mode profile-workload \
    --input "$WIKI" \
    --limit "$DOCS" \
    --index "$INDEX" \
    --profile-scenario "$scenario" \
    --profile-kind mem \
    --profile-iterations "$MEM_ITERATIONS" \
    --profile-output "$out_path"

  env GOCACHE="$GOCACHE" GOMODCACHE="$GOMODCACHE" \
    go tool pprof -top "$APP_BIN" "$out_path" \
    > "${out_path%.out}_top.txt"
done

echo "[OK] Memory profiles saved to $PROFILE_DIR"
