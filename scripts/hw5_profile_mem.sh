#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/reports/hw5/profiles"
cd "$ROOT/hw5"
go test ./... -bench=BenchmarkAndQuery -memprofile ../reports/hw5/profiles/mem_query.out
go tool pprof -top ../reports/hw5/profiles/mem_query.out > ../reports/hw5/profiles/mem_query_top.txt
