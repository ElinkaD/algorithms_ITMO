#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILES_ROOT="$ROOT/reports/hw5/profiles"
APP_BIN="$PROFILES_ROOT/bin/searchdemo"

GOCACHE="${GOCACHE:-/tmp/go-build-hw5}"
GOMODCACHE="${GOMODCACHE:-/tmp/go-mod-cache-hw5}"
PPROF_HTTP="${PPROF_HTTP:-:8080}"

usage() {
  cat <<'EOF'
Usage:
  scripts/hw5_pprof_ui.sh <cpu|mem> <profile-name-or-path>

Examples:
  scripts/hw5_pprof_ui.sh cpu wiki-near-query
  scripts/hw5_pprof_ui.sh mem clean_wiki-bm25-topk.out
  PPROF_HTTP=:8081 scripts/hw5_pprof_ui.sh cpu reports/hw5/profiles/cpu/clean_wiki-near-query.out
EOF
}

if [[ $# -ne 2 ]]; then
  usage
  exit 1
fi

kind="$1"
profile_arg="$2"

case "$kind" in
  cpu|mem) ;;
  *)
    echo "Unknown profile kind: $kind"
    usage
    exit 1
    ;;
esac

if [[ -f "$profile_arg" ]]; then
  profile_path="$profile_arg"
else
  profile_name="${profile_arg%.out}"
  if [[ "$profile_name" != clean_* ]]; then
    profile_name="clean_${profile_name}"
  fi
  profile_path="$PROFILES_ROOT/$kind/${profile_name}.out"
fi

if [[ ! -f "$profile_path" ]]; then
  echo "Profile not found: $profile_path"
  exit 1
fi

if [[ ! -x "$APP_BIN" ]]; then
  echo "Missing binary: $APP_BIN"
  echo "Run scripts/hw5_profile_cpu.sh, scripts/hw5_profile_mem.sh or scripts/hw5_profile_interesting.sh first."
  exit 1
fi

echo "Opening pprof Web UI on http://127.0.0.1${PPROF_HTTP}"
echo "Binary:  $APP_BIN"
echo "Profile: $profile_path"

env GOCACHE="$GOCACHE" GOMODCACHE="$GOMODCACHE" \
  go tool pprof -http="127.0.0.1${PPROF_HTTP}" "$APP_BIN" "$profile_path"
