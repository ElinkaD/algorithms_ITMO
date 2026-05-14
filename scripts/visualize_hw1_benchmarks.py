from pathlib import Path
import os

import matplotlib.pyplot as plt
import pandas as pd


OPERATION_CONFIG = {
    "extendible": {
        "time": [
            ("insert", "insert", "#1f77b4"),
            ("update", "update", "#ff7f0e"),
            ("get", "get", "#2ca02c"),
            ("delete", "delete", "#d62728"),
        ],
        "raw": [
            ("insert", "insert", "#1f77b4"),
            ("update", "update", "#ff7f0e"),
            ("get", "get", "#2ca02c"),
            ("delete", "delete", "#d62728"),
        ],
        "title": "extendible hashing on filesystem buckets",
    },
    "perfect": {
        "time": [
            ("build", "build", "#1f77b4"),
        ],
        "raw": [
            ("build", "build", "#1f77b4"),
            ("get", "get", "#2ca02c"),
        ],
        "title": "perfect hash for a fixed key set",
    },
    "lsh": {
        "time": [],
        "raw": [
            ("build", "build", "#1f77b4"),
            ("add", "add", "#ff7f0e"),
            ("add_one", "add one", "#9467bd"),
            ("search", "search", "#2ca02c"),
            ("fullscan", "full scan", "#d62728"),
        ],
        "title": "lsh for 3d points",
    },
}


def require_env(name: str) -> Path:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return Path(value)


def setup_style() -> None:
    plt.style.use("default")
    plt.rcParams.update(
        {
            "figure.figsize": (11, 6.5),
            "axes.grid": True,
            "grid.alpha": 0.25,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titlesize": 14,
            "axes.labelsize": 12,
            "legend.frameon": False,
            "font.size": 11,
        }
    )


def load_tsv(path: Path, columns: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", header=None, names=columns, dtype=str)
    for column in columns:
        df[column] = pd.to_numeric(df[column].str.replace(",", ".", regex=False), errors="raise")
    return df


def save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def set_size_ticks(ax, sizes) -> None:
    tick_labels = {
        1000: "1k",
        10000: "10k",
        50000: "50k",
        100000: "100k",
        500000: "500k",
        1000000: "1M",
    }
    present = sorted({int(size) for size in sizes if int(size) in tick_labels})
    if not present:
        present = sorted({int(size) for size in sizes})
    ax.set_xticks(present, labels=[tick_labels.get(tick, str(tick)) for tick in present])


def maybe_set_log_scale(ax, values, axis: str) -> None:
    if all(float(value) > 0 for value in values):
        if axis == "x":
            ax.set_xscale("log")
        elif axis == "y":
            ax.set_yscale("log")


def load_get_report(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    numeric_columns = ["rows", "iterations", "get_sec", "get_avg_sec", "get_ci95_avg_sec", "get_ops_sec"]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="raise")
    return df.sort_values("rows")


def plot_perfect_time(summary_dir: Path, raw_dir: Path, out_dir: Path) -> None:
    build_summary = summary_dir / "build_summary.tsv"
    if build_summary.exists():
        fig, ax = plt.subplots()
        df = load_tsv(build_summary, ["size", "mean", "ci", "runs"]).sort_values("size")
        x = df["size"].to_numpy()
        y = df["mean"].to_numpy()
        ci = df["ci"].to_numpy()

        ax.plot(x, y, marker="o", linewidth=2.4, color="#1f77b4", label="build")
        ax.fill_between(x, y - ci, y + ci, alpha=0.18, color="#1f77b4")
        maybe_set_log_scale(ax, x, "x")
        set_size_ticks(ax, x)
        ax.set_xlabel("dataset size")
        ax.set_ylabel("build ns/op")
        ax.set_title("perfect hash build: mean and 95% ci")
        ax.legend(loc="upper left")
        save(fig, out_dir / "build_time_pretty.png")

    get_report = raw_dir / "get_report.csv"
    if get_report.exists():
        fig, ax = plt.subplots()
        df = load_get_report(get_report)
        x = df["rows"].to_numpy()
        y = (df["get_avg_sec"] * 1e9).to_numpy()
        ci = (df["get_ci95_avg_sec"] * 1e9).to_numpy()

        ax.plot(x, y, marker="o", linewidth=2.4, color="#2ca02c", label="get")
        ax.fill_between(x, y - ci, y + ci, alpha=0.18, color="#2ca02c")
        maybe_set_log_scale(ax, x, "x")
        set_size_ticks(ax, x)
        ax.set_xlabel("dataset size")
        ax.set_ylabel("get ns/op")
        ax.set_title("perfect hash get: mean and 95% ci")
        ax.legend(loc="upper left")
        save(fig, out_dir / "get_time_pretty.png")


def lsh_query_count(size: int) -> int:
    count = size // 1000
    if count < 50:
        return 50
    if count > 200:
        return 200
    return count


def plot_lsh_time(summary_dir: Path, out_dir: Path) -> None:
    build_summary = summary_dir / "build_summary.tsv"
    add_summary = summary_dir / "add_summary.tsv"
    add_one_summary = summary_dir / "add_one_summary.tsv"
    search_summary = summary_dir / "search_summary.tsv"
    fullscan_summary = summary_dir / "fullscan_summary.tsv"

    if build_summary.exists() or add_summary.exists():
        fig, ax = plt.subplots()
        used_sizes = []

        if build_summary.exists():
            df = load_tsv(build_summary, ["size", "mean", "ci", "runs"]).sort_values("size")
            x = df["size"].to_numpy()
            y = (df["mean"] / df["size"]).to_numpy()
            ci = (df["ci"] / df["size"]).to_numpy()
            used_sizes.extend(df["size"].tolist())
            ax.plot(x, y, marker="o", linewidth=2.4, color="#1f77b4", label="build (ns/point)")
            ax.fill_between(x, y - ci, y + ci, alpha=0.18, color="#1f77b4")

        if add_summary.exists():
            df = load_tsv(add_summary, ["size", "mean", "ci", "runs"]).sort_values("size")
            batch_sizes = (df["size"] // 10).clip(lower=1)
            x = df["size"].to_numpy()
            y = (df["mean"] / batch_sizes).to_numpy()
            ci = (df["ci"] / batch_sizes).to_numpy()
            used_sizes.extend(df["size"].tolist())
            ax.plot(x, y, marker="o", linewidth=2.4, color="#ff7f0e", label="add 10% batch (ns/point)")
            ax.fill_between(x, y - ci, y + ci, alpha=0.18, color="#ff7f0e")

        maybe_set_log_scale(ax, used_sizes, "x")
        set_size_ticks(ax, used_sizes)
        ax.set_xlabel("dataset size")
        ax.set_ylabel("ns/point")
        ax.set_title("lsh indexing: build vs add")
        ax.legend(loc="upper left")
        save(fig, out_dir / "index_time_pretty.png")

    if add_one_summary.exists():
        fig, ax = plt.subplots()
        df = load_tsv(add_one_summary, ["size", "mean", "ci", "runs"]).sort_values("size")
        x = df["size"].to_numpy()
        y = df["mean"].to_numpy()
        ci = df["ci"].to_numpy()

        ax.plot(x, y, marker="o", linewidth=2.4, color="#9467bd", label="add one point")
        ax.fill_between(x, y - ci, y + ci, alpha=0.18, color="#9467bd")
        maybe_set_log_scale(ax, x, "x")
        maybe_set_log_scale(ax, y, "y")
        set_size_ticks(ax, x)
        ax.set_xlabel("dataset size")
        ax.set_ylabel("ns/op")
        ax.set_title("lsh incremental add: one point")
        ax.legend(loc="upper left")
        save(fig, out_dir / "add_one_time_pretty.png")

    if search_summary.exists() or fullscan_summary.exists():
        fig, ax = plt.subplots()
        used_sizes = []

        if search_summary.exists():
            df = load_tsv(search_summary, ["size", "mean", "ci", "runs"]).sort_values("size")
            queries = df["size"].astype(int).map(lsh_query_count)
            x = df["size"].to_numpy()
            y = (df["mean"] / queries).to_numpy()
            ci = (df["ci"] / queries).to_numpy()
            used_sizes.extend(df["size"].tolist())
            ax.plot(x, y, marker="o", linewidth=2.4, color="#2ca02c", label="search (ns/query)")
            ax.fill_between(x, y - ci, y + ci, alpha=0.18, color="#2ca02c")

        if fullscan_summary.exists():
            df = load_tsv(fullscan_summary, ["size", "mean", "ci", "runs"]).sort_values("size")
            queries = df["size"].astype(int).map(lsh_query_count)
            x = df["size"].to_numpy()
            y = (df["mean"] / queries).to_numpy()
            ci = (df["ci"] / queries).to_numpy()
            used_sizes.extend(df["size"].tolist())
            ax.plot(x, y, marker="o", linewidth=2.4, color="#d62728", label="full scan (ns/query)")
            ax.fill_between(x, y - ci, y + ci, alpha=0.18, color="#d62728")

        maybe_set_log_scale(ax, used_sizes, "x")
        set_size_ticks(ax, used_sizes)
        ax.set_xlabel("dataset size")
        ax.set_ylabel("ns/query")
        ax.set_title("lsh search: indexed vs full scan")
        ax.legend(loc="upper left")
        save(fig, out_dir / "search_time_pretty.png")


def plot_time(summary_dir: Path, out_dir: Path, algo: str) -> None:
    fig, ax = plt.subplots()
    used_sizes = []

    for operation, label, color in OPERATION_CONFIG[algo]["time"]:
        summary_path = summary_dir / f"{operation}_summary.tsv"
        if not summary_path.exists():
            continue

        df = load_tsv(summary_path, ["size", "mean", "ci", "runs"]).sort_values("size")
        used_sizes.extend(df["size"].tolist())
        x = df["size"].to_numpy()
        y = df["mean"].to_numpy()
        ci = df["ci"].to_numpy()

        ax.plot(x, y, marker="o", linewidth=2.2, label=label, color=color)
        ax.fill_between(x, y - ci, y + ci, alpha=0.18, color=color)

    maybe_set_log_scale(ax, used_sizes, "x")
    maybe_set_log_scale(ax, ax.get_lines()[0].get_ydata() if ax.get_lines() else [1], "y")
    set_size_ticks(ax, used_sizes)
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ns/op")
    ax.set_title(f"{OPERATION_CONFIG[algo]['title']}: mean and 95% ci")
    ax.legend(loc="upper left")

    save(fig, out_dir / "time_pretty.png")


def plot_raw_metric(raw_dir: Path, out_dir: Path, algo: str, metric: str, out_name: str, y_label: str) -> None:
    fig, ax = plt.subplots()
    used_sizes = []

    for operation, label, color in OPERATION_CONFIG[algo]["raw"]:
        raw_path = raw_dir / f"{operation}_runs.tsv"
        if not raw_path.exists():
            continue

        df = load_tsv(raw_path, ["run", "size", "ns", "bytes", "allocs"])
        grouped = df.groupby("size")[metric].mean().reset_index().sort_values("size")
        used_sizes.extend(grouped["size"].tolist())
        ax.plot(grouped["size"], grouped[metric], marker="o", linewidth=2.2, label=label, color=color)

    maybe_set_log_scale(ax, used_sizes, "x")
    if ax.get_lines():
        all_y = []
        for line in ax.get_lines():
            all_y.extend(line.get_ydata())
        maybe_set_log_scale(ax, all_y, "y")
    set_size_ticks(ax, used_sizes)
    ax.set_xlabel("dataset size")
    ax.set_ylabel(y_label)
    ax.set_title(f"{OPERATION_CONFIG[algo]['title']}: {y_label}")
    ax.legend(loc="upper left")

    save(fig, out_dir / out_name)


def main() -> None:
    algo = os.environ.get("ALGO", "").strip()
    if algo not in OPERATION_CONFIG:
        raise RuntimeError(f"unsupported ALGO: {algo}")

    summary_dir = require_env("HW1_SUMMARY_DIR")
    raw_dir = require_env("HW1_RAW_DIR")
    out_dir = require_env("HW1_OUT_DIR")

    setup_style()
    if algo == "perfect":
        legacy_plot = out_dir / "time_pretty.png"
        if legacy_plot.exists():
            legacy_plot.unlink()
        plot_perfect_time(summary_dir, raw_dir, out_dir)
    elif algo == "lsh":
        legacy_plot = out_dir / "time_pretty.png"
        if legacy_plot.exists():
            legacy_plot.unlink()
        plot_lsh_time(summary_dir, out_dir)
    else:
        plot_time(summary_dir, out_dir, algo)
    plot_raw_metric(raw_dir, out_dir, algo, "allocs", "allocs_pretty.png", "allocs/op")
    plot_raw_metric(raw_dir, out_dir, algo, "bytes", "bytes_pretty.png", "B/op")


if __name__ == "__main__":
    main()
