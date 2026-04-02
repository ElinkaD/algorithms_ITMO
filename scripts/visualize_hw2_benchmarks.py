from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
HW2_DIR = REPO_ROOT / "hw2"
SUMMARY_DIR = HW2_DIR / "artifacts" / "metrics" / "summary"
RAW_DIR = HW2_DIR / "artifacts" / "metrics" / "raw"
OUT_DIR = REPO_ROOT / "graphs" / "hw2"


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


def load_summary(name: str, columns: list[str]) -> pd.DataFrame:
    df = pd.read_csv(SUMMARY_DIR / name, sep="\t", header=None, names=columns, dtype=str)
    for column in columns:
        df[column] = pd.to_numeric(df[column].str.replace(",", ".", regex=False), errors="raise")
    return df


def load_raw(name: str, columns: list[str]) -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / name, sep="\t", header=None, names=columns, dtype=str)
    for column in columns:
        df[column] = pd.to_numeric(df[column].str.replace(",", ".", regex=False), errors="raise")
    return df


def save(fig: plt.Figure, name: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUT_DIR / name, dpi=220, bbox_inches="tight")
    plt.close(fig)


def set_size_ticks(ax, sizes) -> None:
    known_ticks = [1000, 10000, 50000, 100000, 500000, 1000000]
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
        present = known_ticks[:4]
    ax.set_xticks(present, labels=[tick_labels[tick] for tick in present])


def plot_line_with_ci(ax, df, x_col, y_col, ci_col, label, color):
    ordered = df.sort_values(x_col)
    x = ordered[x_col].to_numpy()
    y = ordered[y_col].to_numpy()
    ci = ordered[ci_col].to_numpy()
    ax.plot(x, y, marker="o", linewidth=2.2, label=label, color=color)
    ax.fill_between(x, y - ci, y + ci, alpha=0.18, color=color)


def plot_nearby_vs_fullscan(radius: int, precision: int, out_name: str, title: str) -> None:
    nearby = load_summary(
        "search_nearby_summary.tsv",
        ["size", "precision", "radius", "mean", "ci", "runs"],
    )
    fullscan = load_summary(
        "fullscan_summary.tsv",
        ["size", "radius", "mean", "ci", "runs"],
    )

    nearby = nearby[(nearby["radius"] == radius) & (nearby["precision"] == precision)]
    fullscan = fullscan[fullscan["radius"] == radius]

    fig, ax = plt.subplots()
    plot_line_with_ci(ax, nearby, "size", "mean", "ci", f"search nearby, precision={precision}", "#1f77b4")
    plot_line_with_ci(ax, fullscan, "size", "mean", "ci", "full scan", "#d62728")

    ax.set_xscale("log")
    ax.set_yscale("log")
    set_size_ticks(ax, pd.concat([nearby["size"], fullscan["size"]]))
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ns/op")
    ax.set_title(title)
    ax.legend(loc="upper left")

    save(fig, out_name)


def plot_exact() -> None:
    exact = load_summary(
        "search_exact_summary.tsv",
        ["size", "mean", "ci", "runs"],
    )

    fig, ax = plt.subplots()
    plot_line_with_ci(ax, exact, "size", "mean", "ci", "search exact", "#2ca02c")
    ax.set_xscale("log")
    ax.set_yscale("log")
    set_size_ticks(ax, exact["size"])
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ns/op")
    ax.set_title("search exact: mean and 95% ci")
    ax.legend(loc="upper left")

    save(fig, "search_exact_pretty.png")


def plot_insert() -> None:
    insert = load_summary(
        "insert_summary.tsv",
        ["size", "mean", "ci", "runs"],
    )

    fig, ax = plt.subplots()
    plot_line_with_ci(ax, insert, "size", "mean", "ci", "insert", "#9467bd")
    ax.set_xscale("log")
    ax.set_yscale("log")
    set_size_ticks(ax, insert["size"])
    ax.set_xlabel("dataset size")
    ax.set_ylabel("ns/op")
    ax.set_title("insert: mean and 95% ci")
    ax.legend(loc="upper left")

    save(fig, "insert_pretty.png")


def plot_memory(radius: int, out_name: str, metric: str, title: str) -> None:
    nearby = load_raw(
        "search_nearby_runs.tsv",
        ["run", "size", "precision", "radius", "ns", "bytes", "allocs"],
    )
    fullscan = load_raw(
        "fullscan_runs.tsv",
        ["run", "size", "radius", "ns", "bytes", "allocs"],
    )

    fig, ax = plt.subplots()
    for precision, color in [(4, "#1f77b4"), (5, "#ff7f0e")]:
        part = nearby[(nearby["radius"] == radius) & (nearby["precision"] == precision)]
        grouped = part.groupby("size")[metric].mean().reset_index().sort_values("size")
        ax.plot(grouped["size"], grouped[metric], marker="o", linewidth=2.2, label=f"search nearby, p={precision}", color=color)

    full_grouped = fullscan[fullscan["radius"] == radius].groupby("size")[metric].mean().reset_index().sort_values("size")
    ax.plot(full_grouped["size"], full_grouped[metric], marker="o", linewidth=2.2, label="full scan", color="#d62728")

    ax.set_xscale("log")
    set_size_ticks(ax, pd.concat([nearby["size"], fullscan["size"]]))
    ax.set_xlabel("dataset size")
    ax.set_ylabel(metric)
    ax.set_title(title)
    ax.legend(loc="upper left")

    save(fig, out_name)


def main() -> None:
    setup_style()
    plot_insert()
    plot_exact()
    plot_nearby_vs_fullscan(
        radius=1000,
        precision=4,
        out_name="nearby_vs_fullscan_radius_1000_p4_pretty.png",
        title="search nearby vs full scan, radius=1000, precision=4",
    )
    plot_nearby_vs_fullscan(
        radius=1000,
        precision=5,
        out_name="nearby_vs_fullscan_radius_1000_p5_pretty.png",
        title="search nearby vs full scan, radius=1000, precision=5",
    )
    plot_nearby_vs_fullscan(
        radius=100000,
        precision=4,
        out_name="nearby_vs_fullscan_radius_100000_p4_pretty.png",
        title="search nearby vs full scan, radius=100000, precision=4",
    )
    plot_nearby_vs_fullscan(
        radius=100000,
        precision=5,
        out_name="nearby_vs_fullscan_radius_100000p5_pretty.png",
        title="search nearby vs full scan, radius=100000, precision=5",
    )
    plot_memory(
        radius=100000,
        out_name="nearby_allocs_radius_100000_pretty.png",
        metric="allocs",
        title="allocations per operation, radius=100000",
    )
    plot_memory(
        radius=100000,
        out_name="nearby_bytes_radius_100000_pretty.png",
        metric="bytes",
        title="memory per operation, radius=100000",
    )


if __name__ == "__main__":
    main()
