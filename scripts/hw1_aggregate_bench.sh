#!/usr/bin/env bash

set -euo pipefail

raw_dir="${1:-}"
summary_dir="${2:-}"

if [[ -z "${raw_dir}" || -z "${summary_dir}" ]]; then
  echo "usage: $0 <raw-dir> <summary-dir>" >&2
  exit 1
fi

mkdir -p "${summary_dir}"

shopt -s nullglob
for input_file in "${raw_dir}"/*_runs.tsv; do
  base_name="$(basename "${input_file}" _runs.tsv)"
  output_file="${summary_dir}/${base_name}_summary.tsv"

  awk -F'\t' 'BEGIN { OFS="\t" }
    {
      size = $2
      value = $3
      values[size, ++count[size]] = value
    }
    END {
      for (size in count) {
        n = count[size]
        sum = 0
        for (i = 1; i <= n; i++) {
          sum += values[size, i]
        }
        mean = sum / n

        sq = 0
        for (i = 1; i <= n; i++) {
          diff = values[size, i] - mean
          sq += diff * diff
        }

        sd = (n > 1) ? sqrt(sq / (n - 1)) : 0
        ci = (n > 1) ? 1.96 * sd / sqrt(n) : 0
        print size, mean, ci, n
      }
    }' "${input_file}" | sort -n > "${output_file}"
done
