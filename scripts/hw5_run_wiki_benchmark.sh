#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCS="${DOCS:-5000}"
WIKI="${WIKI:-./data/wiki_sample.jsonl}"
INDEX="${INDEX:-./data/wiki.seg}"
ITERATIONS="${ITERATIONS:-5}"

cd "$ROOT/hw5"
make wiki-stats DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make build-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make prepare-wiki-queries DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make compression-stats-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make bench-query-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX" ITERATIONS="$ITERATIONS"
make bench-mmap-vs-memory DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX" ITERATIONS="$ITERATIONS"
make bench-ranking-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX" ITERATIONS="$ITERATIONS"
