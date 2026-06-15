#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCS="${DOCS:-5000}"
WIKI="${WIKI:-./data/wiki_sample.jsonl}"
INDEX="${INDEX:-./data/wiki.seg}"
ITERATIONS="${ITERATIONS:-5}"
TOPK="${TOPK:-10}"

cd "$ROOT/hw5"
make wiki-stats DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make bench-build-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX" ITERATIONS="$ITERATIONS"
make prepare-wiki-queries DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make compression-stats-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make bench-query-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX" ITERATIONS="$ITERATIONS"
make bench-ranking-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX" TOPK="$TOPK" ITERATIONS="$ITERATIONS"
