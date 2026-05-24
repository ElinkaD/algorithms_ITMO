from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
HW4_DIR = REPO_ROOT / "hw4"
SUMMARY_DIR = HW4_DIR / "artifacts" / "metrics" / "summary"
OUT_DIR = REPO_ROOT / "graphs" / "hw4"


def setup_style() -> None:
    plt.style.use("default")
    plt.rcParams.update(
        {
            "figure.figsize": (11.5, 6.8),
            "axes.grid": True,
            "grid.alpha": 0.22,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titlesize": 14,
            "axes.labelsize": 12,
            "legend.frameon": False,
            "font.size": 11,
            "lines.markersize": 6,
        }
    )


def load_summary(name: str) -> pd.DataFrame:
    return pd.read_csv(SUMMARY_DIR / name, sep="\t")


def save(fig: plt.Figure, name: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUT_DIR / name, dpi=220, bbox_inches="tight")
    plt.close(fig)


def set_size_ticks(ax, sizes) -> None:
    tick_labels = {
        1000: "1k",
        2500: "2.5k",
        5000: "5k",
        10000: "10k",
        25000: "25k",
        50000: "50k",
        100000: "100k",
    }
    present = sorted({int(size) for size in sizes if int(size) in tick_labels})
    ax.set_xticks(present, labels=[tick_labels[tick] for tick in present])
    ax.tick_params(axis="x", rotation=0)


def plot_line_with_ci(ax, df, label, color):
    ordered = df.sort_values("size")
    x = ordered["size"].to_numpy()
    y = ordered["ns_mean"].to_numpy()
    ci = ordered["ns_ci95"].to_numpy()
    ax.plot(x, y, marker="o", linewidth=2.2, label=label, color=color)
    ax.fill_between(x, y - ci, y + ci, alpha=0.18, color=color)


def plot_memory_line(ax, df, y_col: str, label: str, color: str):
    ordered = df.sort_values("size")
    x = ordered["size"].to_numpy()
    y = ordered[y_col].to_numpy()
    ax.plot(x, y, marker="o", linewidth=2.2, label=label, color=color)


def plot_put_get_compare() -> None:
    put_concurrent = load_summary("put_concurrent_summary.tsv")
    put_plain = load_summary("put_plain_summary.tsv")
    get_concurrent = load_summary("get_concurrent_summary.tsv")
    get_plain = load_summary("get_plain_summary.tsv")

    fig, ax = plt.subplots()
    plot_line_with_ci(ax, put_concurrent, "put concurrent", "#1f77b4")
    plot_line_with_ci(ax, put_plain, "put plain", "#ff7f0e")
    plot_line_with_ci(ax, get_concurrent, "get concurrent", "#2ca02c")
    plot_line_with_ci(ax, get_plain, "get plain", "#d62728")
    ax.set_xscale("log")
    ax.set_yscale("log")
    set_size_ticks(ax, pd.concat([put_concurrent["size"], put_plain["size"]]))
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ns/op")
    ax.set_title("put/get: concurrent vs plain")
    ax.legend(loc="upper left", ncols=2)
    save(fig, "put_get_latency_pretty.png")


def plot_merge_compare() -> None:
    merge_concurrent = load_summary("merge_concurrent_summary.tsv")
    merge_plain = load_summary("merge_plain_summary.tsv")

    fig, ax = plt.subplots()
    plot_line_with_ci(ax, merge_concurrent, "merge concurrent", "#9467bd")
    plot_line_with_ci(ax, merge_plain, "merge plain", "#8c564b")
    ax.set_xscale("log")
    ax.set_yscale("log")
    set_size_ticks(ax, merge_concurrent["size"])
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ns/op")
    ax.set_title("merge hit: concurrent vs plain")
    ax.legend(loc="upper left")
    save(fig, "merge_latency_pretty.png")


def plot_parallel_workloads() -> None:
    read_mostly = load_summary("read_mostly_summary.tsv")
    put_parallel = load_summary("put_parallel_summary.tsv")
    hot_key = load_summary("merge_parallel_same_key_summary.tsv")

    fig, ax = plt.subplots()
    plot_line_with_ci(ax, read_mostly, "read mostly", "#1f77b4")
    plot_line_with_ci(ax, put_parallel, "parallel put", "#ff7f0e")
    plot_line_with_ci(ax, hot_key, "parallel merge same key", "#2ca02c")
    ax.set_xscale("log")
    ax.set_yscale("log")
    set_size_ticks(ax, read_mostly["size"])
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ns/op")
    ax.set_title("parallel workloads")
    ax.legend(loc="upper left")
    save(fig, "parallel_workloads_pretty.png")


def plot_put_throughput() -> None:
    put_concurrent = load_summary("put_concurrent_summary.tsv")
    put_plain = load_summary("put_plain_summary.tsv")

    fig, ax = plt.subplots()
    for df, label, color in [
        (put_concurrent, "put concurrent", "#1f77b4"),
        (put_plain, "put plain", "#ff7f0e"),
    ]:
        ordered = df.sort_values("size")
        x = ordered["size"].to_numpy()
        y = ordered["throughput_mean"].to_numpy()
        ci = ordered["throughput_ci95"].to_numpy()
        ax.plot(x, y, marker="o", linewidth=2.2, label=label, color=color)
        ax.fill_between(x, y - ci, y + ci, alpha=0.18, color=color)

    ax.set_xscale("log")
    ax.set_yscale("log")
    set_size_ticks(ax, put_concurrent["size"])
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ops/sec")
    ax.set_title("put throughput")
    ax.legend(loc="upper left")
    save(fig, "put_throughput_pretty.png")


def plot_memory() -> None:
    put_concurrent = load_summary("put_concurrent_summary.tsv")
    put_plain = load_summary("put_plain_summary.tsv")

    fig, ax = plt.subplots()
    plot_memory_line(ax, put_concurrent, "bytes_mean", "put concurrent", "#1f77b4")
    plot_memory_line(ax, put_plain, "bytes_mean", "put plain", "#ff7f0e")
    ax.set_xscale("log")
    set_size_ticks(ax, put_concurrent["size"])
    ax.set_xlabel("dataset size")
    ax.set_ylabel("B/op")
    ax.set_title("memory per put")
    ax.legend(loc="upper left")
    save(fig, "put_memory_pretty.png")


def plot_allocs() -> None:
    put_concurrent = load_summary("put_concurrent_summary.tsv")
    put_plain = load_summary("put_plain_summary.tsv")

    fig, ax = plt.subplots()
    plot_memory_line(ax, put_concurrent, "allocs_mean", "put concurrent", "#1f77b4")
    plot_memory_line(ax, put_plain, "allocs_mean", "put plain", "#ff7f0e")
    ax.set_xscale("log")
    set_size_ticks(ax, put_concurrent["size"])
    ax.set_xlabel("dataset size")
    ax.set_ylabel("allocs/op")
    ax.set_title("allocations per operation")
    ax.legend(loc="upper left")
    save(fig, "allocs_pretty.png")


def main() -> None:
    setup_style()
    plot_put_get_compare()
    plot_merge_compare()
    plot_parallel_workloads()
    plot_put_throughput()
    plot_memory()
    plot_allocs()


if __name__ == "__main__":
    main()
