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

while IFS= read -r line; do
  [[ "${line}" == Benchmark* ]] || continue

  read -r -a fields <<< "${line}"
  name="${fields[0]}"
  nsop=""
  bytes=""
  allocs=""

  for ((i = 0; i < ${#fields[@]}; i++)); do
    if [[ "${fields[i]}" == "ns/op" && $i -gt 0 ]]; then
      nsop="${fields[i-1]}"
    fi
    if [[ "${fields[i]}" == "B/op" && $i -gt 0 ]]; then
      bytes="${fields[i-1]}"
    fi
    if [[ "${fields[i]}" == "allocs/op" && $i -gt 0 ]]; then
      allocs="${fields[i-1]}"
    fi
  done

  [[ -n "${nsop}" && -n "${bytes}" && -n "${allocs}" ]] || continue

  case "${name}" in
    BenchmarkMapPut/concurrent/size=*)
      size="${name#BenchmarkMapPut/concurrent/size=}"
      size="${size%-*}"
      append_result "${output_dir}/put_concurrent_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapPut/plain/size=*)
      size="${name#BenchmarkMapPut/plain/size=}"
      size="${size%-*}"
      append_result "${output_dir}/put_plain_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapGetHit/concurrent/size=*)
      size="${name#BenchmarkMapGetHit/concurrent/size=}"
      size="${size%-*}"
      append_result "${output_dir}/get_concurrent_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapGetHit/plain/size=*)
      size="${name#BenchmarkMapGetHit/plain/size=}"
      size="${size%-*}"
      append_result "${output_dir}/get_plain_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapMergeHit/concurrent/size=*)
      size="${name#BenchmarkMapMergeHit/concurrent/size=}"
      size="${size%-*}"
      append_result "${output_dir}/merge_concurrent_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapMergeHit/plain/size=*)
      size="${name#BenchmarkMapMergeHit/plain/size=}"
      size="${size%-*}"
      append_result "${output_dir}/merge_plain_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapReadMostlyParallel/size=*)
      size="${name#BenchmarkMapReadMostlyParallel/size=}"
      size="${size%-*}"
      append_result "${output_dir}/read_mostly_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapPutParallel/concurrent/size=*)
      size="${name#BenchmarkMapPutParallel/concurrent/size=}"
      size="${size%-*}"
      append_result "${output_dir}/put_parallel_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
    BenchmarkMapMergeParallelSameKey/size=*)
      size="${name#BenchmarkMapMergeParallelSameKey/size=}"
      size="${size%-*}"
      append_result "${output_dir}/merge_parallel_same_key_runs.tsv" "${size}" "${nsop}" "${bytes}" "${allocs}"
      ;;
  esac
done < "${input_file}"
