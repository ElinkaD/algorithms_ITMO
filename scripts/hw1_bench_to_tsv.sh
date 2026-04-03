#!/usr/bin/env bash

set -euo pipefail

algo="${1:-}"
run_id="${2:-}"
input_file="${3:-}"
output_dir="${4:-}"

if [[ -z "${algo}" || -z "${run_id}" || -z "${input_file}" || -z "${output_dir}" ]]; then
  echo "usage: $0 <algo> <run-id> <bench.txt> <output-dir>" >&2
  exit 1
fi

mkdir -p "${output_dir}"

append_result() {
  local file="$1"
  shift
  {
    printf '%s' "${run_id}"
    for field in "$@"; do
      printf '\t%s' "${field}"
    done
    printf '\n'
  } >> "${file}"
}

while read -r name iter nsop _ bytes _ allocs _; do
  [[ "${name}" == Benchmark* ]] || continue

  case "${algo}:${name}" in
    extendible:BenchmarkTableInsert/size=*)
      size="${name#BenchmarkTableInsert/size=}"
      size="${size%-*}"
      append_result "${output_dir}/insert_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    extendible:BenchmarkTableUpdate/size=*)
      size="${name#BenchmarkTableUpdate/size=}"
      size="${size%-*}"
      append_result "${output_dir}/update_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    extendible:BenchmarkTableGet/size=*)
      size="${name#BenchmarkTableGet/size=}"
      size="${size%-*}"
      append_result "${output_dir}/get_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    extendible:BenchmarkTableDelete/size=*)
      size="${name#BenchmarkTableDelete/size=}"
      size="${size%-*}"
      append_result "${output_dir}/delete_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    perfect:BenchmarkTableBuild/size=*)
      size="${name#BenchmarkTableBuild/size=}"
      size="${size%-*}"
      append_result "${output_dir}/build_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    perfect:BenchmarkTableGet/size=*)
      size="${name#BenchmarkTableGet/size=}"
      size="${size%-*}"
      append_result "${output_dir}/get_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    perfect:BenchmarkTableGetMiss/size=*)
      size="${name#BenchmarkTableGetMiss/size=}"
      size="${size%-*}"
      append_result "${output_dir}/get_miss_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    lsh:BenchmarkTableBuild/size=*)
      size="${name#BenchmarkTableBuild/size=}"
      size="${size%-*}"
      append_result "${output_dir}/build_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    lsh:BenchmarkTableAdd/size=*)
      size="${name#BenchmarkTableAdd/size=}"
      size="${size%-*}"
      append_result "${output_dir}/add_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    lsh:BenchmarkTableSearch/size=*)
      size="${name#BenchmarkTableSearch/size=}"
      size="${size%-*}"
      append_result "${output_dir}/search_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    lsh:BenchmarkTableFullScan/size=*)
      size="${name#BenchmarkTableFullScan/size=}"
      size="${size%-*}"
      append_result "${output_dir}/fullscan_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
  esac
done < "${input_file}"
