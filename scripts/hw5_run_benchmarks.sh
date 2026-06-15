#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/reports/hw5/results"
cd "$ROOT/hw5"
go test ./... -bench=. -benchmem | tee ../reports/hw5/results/go_bench_raw.txt
