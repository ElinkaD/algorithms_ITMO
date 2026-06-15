#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "[profile-interesting] CPU profiles"
"$ROOT/scripts/hw5_profile_cpu.sh"
echo "[profile-interesting] MEM profiles"
"$ROOT/scripts/hw5_profile_mem.sh"
echo "[OK] Clean workload profiles saved to $ROOT/reports/hw5/profiles"
