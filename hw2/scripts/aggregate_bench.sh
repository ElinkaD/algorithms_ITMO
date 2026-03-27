#!/usr/bin/env bash

set -euo pipefail

raw_dir="${1:-}"
summary_dir="${2:-}"

if [[ -z "${raw_dir}" || -z "${summary_dir}" ]]; then
  echo "usage: $0 <raw-dir> <summary-dir>" >&2
  exit 1
fi

mkdir -p "${summary_dir}"

aggregate_file() {
  local input_file="$1"
  local output_file="$2"
  local key_cols="$3"

  awk -F'\t' -v OFS='\t' -v key_cols="${key_cols}" '
    function abs(v) { return v < 0 ? -v : v }
    {
      key = $2
      for (i = 3; i <= key_cols + 1; i++) {
        key = key OFS $i
      }

      value = $(key_cols + 2)
      values[key, ++count[key]] = value
    }
    END {
      for (key in count) {
        n = count[key]
        sum = 0
        for (i = 1; i <= n; i++) {
          sum += values[key, i]
        }
        mean = sum / n

        sq = 0
        for (i = 1; i <= n; i++) {
          diff = values[key, i] - mean
          sq += diff * diff
        }

        sd = (n > 1) ? sqrt(sq / (n - 1)) : 0
        ci = (n > 1) ? 1.96 * sd / sqrt(n) : 0

        print key, mean, ci, n
      }
    }
  ' "${input_file}" | sort -n > "${output_file}"
}

aggregate_file "${raw_dir}/insert_runs.tsv" "${summary_dir}/insert_summary.tsv" 1
aggregate_file "${raw_dir}/search_exact_runs.tsv" "${summary_dir}/search_exact_summary.tsv" 1
aggregate_file "${raw_dir}/search_nearby_runs.tsv" "${summary_dir}/search_nearby_summary.tsv" 3
aggregate_file "${raw_dir}/fullscan_runs.tsv" "${summary_dir}/fullscan_summary.tsv" 2
