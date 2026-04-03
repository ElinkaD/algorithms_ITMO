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
        "title": "extendible hashing on filesystem buckets",
    },
    "perfect": {
        "time": [
            ("build", "build", "#1f77b4"),
            ("get", "get hit", "#2ca02c"),
            ("get_miss", "get miss", "#d62728"),
        ],
        "title": "perfect hash for a fixed key set",
    },
    "lsh": {
        "time": [
            ("build", "build", "#1f77b4"),
            ("add", "add", "#ff7f0e"),
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

    for operation, label, color in OPERATION_CONFIG[algo]["time"]:
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
    plot_time(summary_dir, out_dir, algo)
    plot_raw_metric(raw_dir, out_dir, algo, "allocs", "allocs_pretty.png", "allocs/op")
    plot_raw_metric(raw_dir, out_dir, algo, "bytes", "bytes_pretty.png", "B/op")


if __name__ == "__main__":
    main()
