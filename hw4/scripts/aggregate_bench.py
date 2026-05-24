from __future__ import annotations

import math
from pathlib import Path
import sys

import pandas as pd


def summarize_file(path: Path, out_dir: Path) -> None:
    if not path.exists():
        return

    df = pd.read_csv(
        path,
        sep="\t",
        header=None,
        names=["run", "size", "ns", "bytes", "allocs"],
    )
    for column in ("run", "size", "ns", "bytes", "allocs"):
        df[column] = pd.to_numeric(df[column], errors="raise")

    grouped = (
        df.groupby("size", as_index=False)
        .agg(
            ns_mean=("ns", "mean"),
            ns_std=("ns", "std"),
            bytes_mean=("bytes", "mean"),
            bytes_std=("bytes", "std"),
            allocs_mean=("allocs", "mean"),
            allocs_std=("allocs", "std"),
            runs=("run", "count"),
        )
        .sort_values("size")
    )

    grouped = grouped.fillna(0.0)
    grouped["throughput_mean"] = 1e9 / grouped["ns_mean"]
    grouped["ns_ci95"] = grouped.apply(ci95, axis=1, metric="ns_std")
    grouped["throughput_ci95"] = grouped["throughput_mean"] * (grouped["ns_ci95"] / grouped["ns_mean"])
    grouped["bytes_ci95"] = grouped.apply(ci95, axis=1, metric="bytes_std")
    grouped["allocs_ci95"] = grouped.apply(ci95, axis=1, metric="allocs_std")

    out_path = out_dir / path.name.replace("_runs.tsv", "_summary.tsv")
    grouped.to_csv(out_path, sep="\t", index=False, float_format="%.6f")


def ci95(row: pd.Series, metric: str) -> float:
    runs = int(row["runs"])
    if runs <= 1:
        return 0.0
    return 1.96 * float(row[metric]) / math.sqrt(runs)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: aggregate_bench.py <raw-dir> <summary-dir>")

    raw_dir = Path(sys.argv[1])
    summary_dir = Path(sys.argv[2])
    summary_dir.mkdir(parents=True, exist_ok=True)

    for path in sorted(raw_dir.glob("*_runs.tsv")):
        summarize_file(path, summary_dir)


if __name__ == "__main__":
    main()
