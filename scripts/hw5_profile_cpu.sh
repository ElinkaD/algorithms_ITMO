#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/reports/hw5/profiles"
cd "$ROOT/hw5"
go test ./... -bench=BenchmarkAndQuery -cpuprofile ../reports/hw5/profiles/cpu_and.out
go test ./... -bench=BenchmarkOrQuery -cpuprofile ../reports/hw5/profiles/cpu_or.out
go test ./... -bench=BenchmarkNearQuery -cpuprofile ../reports/hw5/profiles/cpu_near.out
go test ./... -bench=BenchmarkBM25TopK -cpuprofile ../reports/hw5/profiles/cpu_bm25.out
go tool pprof -top ../reports/hw5/profiles/cpu_and.out > ../reports/hw5/profiles/cpu_and_top.txt
