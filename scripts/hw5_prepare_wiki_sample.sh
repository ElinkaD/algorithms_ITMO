#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WIKI="${WIKI:-$ROOT/hw5/data/wiki_sample.jsonl}"
TARGET_GB="${TARGET_GB:-6}"
DOCS="${DOCS:-0}"
DOWNLOAD="${DOWNLOAD:-0}"

mkdir -p "$ROOT/hw5/data"

if [[ -f "$WIKI" ]]; then
  echo "[prepare-wiki] found $WIKI"
  du -h "$WIKI" || true
  exit 0
fi

cat <<MSG
[prepare-wiki] $WIKI is missing.

Expected JSONL format:
{"id":1,"title":"Article title","text":"Article text"}

Fast manual preparation:
  mkdir -p hw5/data
  head -n 5000 full_wiki.jsonl > hw5/data/wiki_sample.jsonl

Official Wikimedia subset download, target 5-8GB JSONL:
  DOWNLOAD=1 TARGET_GB=$TARGET_GB scripts/hw5_prepare_wiki_sample.sh

If macOS Python fails with CERTIFICATE_VERIFY_FAILED:
  open "/Applications/Python 3.10/Install Certificates.command"
  # or, for one-off public Wikimedia download:
  WIKI_INSECURE_SSL=1 DOWNLOAD=1 TARGET_GB=$TARGET_GB scripts/hw5_prepare_wiki_sample.sh

This uses official enwiki latest article chunks from:
  https://dumps.wikimedia.org/enwiki/latest/
MSG

if [[ "$DOWNLOAD" != "1" ]]; then
  exit 0
fi

TARGET_GB="$TARGET_GB" DOCS="$DOCS" WIKI_INSECURE_SSL="${WIKI_INSECURE_SSL:-0}" python3 "$ROOT/scripts/hw5_download_wiki_subset.py"
