#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCS="${DOCS:-5000}"
WIKI="${WIKI:-./data/wiki_sample.jsonl}"
INDEX="${INDEX:-./data/wiki.seg}"

cd "$ROOT/hw5"
make prepare-wiki-queries DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
make bench-query-wiki DOCS="$DOCS" WIKI="$WIKI" INDEX="$INDEX"
