#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
venv_dir="${repo_root}/.venv-graphs"

if [[ ! -x "${venv_dir}/bin/python" ]]; then
  python3 -m venv "${venv_dir}"
fi

if ! "${venv_dir}/bin/python" - <<'PY'
import importlib.util
import sys

required = ["matplotlib", "pandas"]
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    sys.exit(1)
PY
then
  "${venv_dir}/bin/pip" install matplotlib pandas
fi
