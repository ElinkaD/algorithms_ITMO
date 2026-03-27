#!/usr/bin/env bash

set -euo pipefail

run_id="${1:-}"
input_file="${2:-}"
output_dir="${3:-}"

if [[ -z "${run_id}" || -z "${input_file}" || -z "${output_dir}" ]]; then
  echo "usage: $0 <run-id> <bench.txt> <output-dir>" >&2
  exit 1
fi

mkdir -p "${output_dir}"

insert_file="${output_dir}/insert_runs.tsv"
exact_file="${output_dir}/search_exact_runs.tsv"
nearby_file="${output_dir}/search_nearby_runs.tsv"
fullscan_file="${output_dir}/fullscan_runs.tsv"

touch "${insert_file}" "${exact_file}" "${nearby_file}" "${fullscan_file}"

while read -r name iter nsop _ bytes _ allocs _; do
  [[ "${name}" == Benchmark* ]] || continue

  case "${name}" in
    BenchmarkInsert/size=*)
      size="${name#BenchmarkInsert/size=}"
      size="${size%-*}"
      printf "%s\t%s\t%s\t%s\t%s\n" "${run_id}" "${size}" "${nsop}" "${bytes}" "${allocs}" >> "${insert_file}"
      ;;
    BenchmarkSearchExact/size=*)
      size="${name#BenchmarkSearchExact/size=}"
      size="${size%-*}"
      printf "%s\t%s\t%s\t%s\t%s\n" "${run_id}" "${size}" "${nsop}" "${bytes}" "${allocs}" >> "${exact_file}"
      ;;
    BenchmarkSearchNearby/size=*"/precision="*"/radius="*)
      meta="${name#BenchmarkSearchNearby/}"
      meta="${meta%-*}"
      size="$(printf '%s' "${meta}" | sed -E 's#size=([0-9]+)/precision=([0-9]+)/radius=([0-9]+)#\1#')"
      precision="$(printf '%s' "${meta}" | sed -E 's#size=([0-9]+)/precision=([0-9]+)/radius=([0-9]+)#\2#')"
      radius="$(printf '%s' "${meta}" | sed -E 's#size=([0-9]+)/precision=([0-9]+)/radius=([0-9]+)#\3#')"
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "${run_id}" "${size}" "${precision}" "${radius}" "${nsop}" "${bytes}" "${allocs}" >> "${nearby_file}"
      ;;
    BenchmarkFullScan/size=*"/radius="*)
      meta="${name#BenchmarkFullScan/}"
      meta="${meta%-*}"
      size="$(printf '%s' "${meta}" | sed -E 's#size=([0-9]+)/radius=([0-9]+)#\1#')"
      radius="$(printf '%s' "${meta}" | sed -E 's#size=([0-9]+)/radius=([0-9]+)#\2#')"
      printf "%s\t%s\t%s\t%s\t%s\t%s\n" "${run_id}" "${size}" "${radius}" "${nsop}" "${bytes}" "${allocs}" >> "${fullscan_file}"
      ;;
  esac
done < "${input_file}"
