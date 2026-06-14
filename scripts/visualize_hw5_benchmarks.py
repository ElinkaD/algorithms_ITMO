#!/usr/bin/env python3
from pathlib import Path
import csv
import os
from collections import defaultdict
import re

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "reports" / "hw5" / "results"
GRAPHS = ROOT / "graphs" / "hw5"
GRAPHS.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", "/tmp/algorithms-itmo-matplotlib")

try:
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit("matplotlib is required: pip install matplotlib") from exc

created = []


def read_csv(name):
    path = RESULTS / name
    if not path.exists():
        print(f"[graphs] skip missing {path}")
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def read_sharded_query_csvs():
    rows = []
    paths = sorted(RESULTS.glob("wiki_sharded_query_latency_docs*_shards*.csv"))
    if not paths:
        paths = sorted(RESULTS.glob("wiki_sharded_query_latency.csv"))
    for path in paths:
        with path.open(newline="") as f:
            part = list(csv.DictReader(f))
        if not part:
            continue
        match = re.search(r"docs(\d+)_shards(\d+)", path.stem)
        if match:
            docs, shards = match.groups()
            shard_label = f"{shards} seg / {docs} docs"
            run_order = int(shards)
        else:
            docs = part[0].get("docs", "")
            shard_label = f"docs{docs}"
            run_order = int(docs) if docs.isdigit() else 0
        for row in part:
            row["run"] = shard_label
            row["run_order"] = str(run_order)
        rows.extend(part)
    return rows


def save(path):
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    created.append(path)


def bar(rows, label_col, value_col, title, ylabel, filename):
    rows = [r for r in rows if r.get(label_col) and r.get(value_col)]
    if not rows:
        print(f"[graphs] skip {filename}: no data")
        return
    labels = [r[label_col] for r in rows]
    values = [float(r[value_col]) for r in rows]
    plt.figure(figsize=(10, 5))
    plt.bar(labels, values, color="#2563eb")
    plt.title(title)
    plt.xlabel(label_col)
    plt.ylabel(ylabel)
    plt.xticks(rotation=30, ha="right")
    save(GRAPHS / filename)


def bar_values(labels, values, title, ylabel, filename, color="#2563eb"):
    if not labels or not values:
        print(f"[graphs] skip {filename}: no data")
        return
    plt.figure(figsize=(10, 5))
    plt.bar(labels, values, color=color)
    plt.title(title)
    plt.xlabel("corpus")
    plt.ylabel(ylabel)
    plt.xticks(rotation=30, ha="right")
    save(GRAPHS / filename)


def grouped_bar(rows, group_col, series_col, value_col, title, ylabel, filename):
    if not rows:
        print(f"[graphs] skip {filename}: no data")
        return
    op_order = ["TERM", "AND", "OR", "NOT", "ADJ", "NEAR/3", "PHRASE", "COMPLEX", "TFIDF_TOPK", "BM25_TOPK"]
    groups = sorted({r[group_col] for r in rows}, key=lambda v: (op_order.index(v) if v in op_order else len(op_order), v))
    series = sorted({r[series_col] for r in rows}, key=lambda v: min((int(r.get("run_order", "0")) for r in rows if r[series_col] == v), default=0))
    values = {(r[group_col], r[series_col]): float(r[value_col]) for r in rows}
    x = list(range(len(groups)))
    width = 0.8 / max(1, len(series))
    plt.figure(figsize=(12, 5))
    for i, s in enumerate(series):
        offset = (i - (len(series) - 1) / 2) * width
        plt.bar([v + offset for v in x], [values.get((g, s), 0) for g in groups], width=width, label=s)
    plt.title(title)
    plt.xlabel(group_col)
    plt.ylabel(ylabel)
    plt.xticks(x, groups, rotation=30, ha="right")
    plt.legend()
    save(GRAPHS / filename)


def sharded_index_size_rows():
    shard_dir = ROOT / "hw5" / "data" / "wiki_shards"
    shards = sorted(shard_dir.glob("shard-*.seg"))
    if not shards:
        return []
    configs = [
        ("1 seg / 50000 docs", 1),
        ("9 seg / 417728 docs", 9),
        ("17 seg / 835456 docs", 17),
    ]
    rows = []
    for label, count in configs:
        if len(shards) < count:
            continue
        size_mb = sum(path.stat().st_size for path in shards[:count]) / 1_000_000
        rows.append({"dataset": label, "index_size_mb": f"{size_mb:.2f}"})
    return rows


wiki_scale = read_csv("wiki_scale_stats.csv")
wiki_comp = read_csv("wiki_compression_stats.csv")
comp = wiki_scale or wiki_comp
if comp:
    bar(comp, "docs", "compression_ratio", "Compression ratio by docs", "raw / compressed", "compression_ratio_by_corpus.png")
    labels = [f"{r.get('docs', '')} docs" for r in comp]
    raw = [float(r["raw_total_bytes"]) / 1_000_000 for r in comp if r.get("raw_total_bytes")]
    compressed = [float(r["compressed_total_bytes"]) / 1_000_000 for r in comp if r.get("compressed_total_bytes")]
    if raw and compressed and len(raw) == len(compressed):
        x = list(range(len(labels)))
        plt.figure(figsize=(10, 5))
        plt.bar([i - 0.2 for i in x], raw, width=0.4, label="raw")
        plt.bar([i + 0.2 for i in x], compressed, width=0.4, label="compressed")
        plt.title("Raw vs compressed size")
        plt.xlabel("corpus")
        plt.ylabel("MB")
        plt.xticks(x, labels, rotation=30, ha="right")
        plt.legend()
        save(GRAPHS / "raw_vs_compressed_size.png")

build = read_csv("wiki_build_stats.csv")
if build:
    labels = [f"{r.get('docs', '')} docs" for r in build]
    build_values = [float(r["total_build_time_ms"]) / 1000 for r in build if r.get("total_build_time_ms")]
    write_values = [float(r["segment_write_time_ms"]) / 1000 for r in build if r.get("segment_write_time_ms")]
    if build_values and write_values and len(build_values) == len(labels) and len(write_values) == len(labels):
        x = list(range(len(labels)))
        plt.figure(figsize=(10, 5))
        plt.bar([i - 0.2 for i in x], build_values, width=0.4, label="build")
        plt.bar([i + 0.2 for i in x], write_values, width=0.4, label="write segment")
        plt.title("Index build breakdown")
        plt.xlabel("corpus")
        plt.ylabel("seconds")
        plt.xticks(x, labels, rotation=30, ha="right")
        plt.legend()
        save(GRAPHS / "build_time_by_docs.png")
    else:
        values = [float(r["total_time_ms"]) / 1000 for r in build if r.get("total_time_ms")]
        bar_values(labels, values, "Build time by docs", "seconds", "build_time_by_docs.png")

sharded_sizes = sharded_index_size_rows()
if sharded_sizes:
    bar(sharded_sizes, "dataset", "index_size_mb", "Sharded index size", "MB", "index_size_by_dataset.png")

sharded_query = read_sharded_query_csvs()
query = sharded_query or read_csv("wiki_query_latency.csv")
if query:
    grouped = {}
    for r in query:
        op = r.get("operator_type", "")
        if not op:
            continue
        grouped.setdefault(op, []).append(r)
    op_rows = []
    for op, rows in sorted(grouped.items()):
        lat = sum(float(r.get("avg_latency_ms") or 0) for r in rows) / len(rows)
        qps = sum(float(r.get("qps") or 0) for r in rows) / len(rows)
        allocs = sum(float(r.get("allocs_per_query") or 0) for r in rows) / len(rows)
        op_rows.append({"operator_type": op, "avg_latency_ms": lat, "qps": qps, "allocs_per_query": allocs})
    bar(op_rows, "operator_type", "avg_latency_ms", "Query latency by operator", "ms", "query_latency_by_operator.png")
    bar(op_rows, "operator_type", "qps", "QPS by operator", "queries/sec", "qps_by_operator.png")
    bar(op_rows, "operator_type", "allocs_per_query", "Allocations by operator", "allocs/query", "allocs_by_operator.png")
    if sharded_query:
        by_run = defaultdict(list)
        for row in sharded_query:
            by_run[(row["run"], row["operator_type"])].append(row)
        scale_rows = []
        for (run, op), rows in sorted(by_run.items()):
            scale_rows.append({
                "run": run,
                "operator_type": op,
                "avg_latency_ms": sum(float(r["avg_latency_ms"]) for r in rows) / len(rows),
                "qps": sum(float(r["qps"]) for r in rows) / len(rows),
            })
        grouped_bar(scale_rows, "operator_type", "run", "avg_latency_ms", "Sharded latency by run", "ms", "sharded_latency_by_operator.png")
        run_rows = []
        for run in sorted({r["run"] for r in sharded_query}):
            rows = [r for r in sharded_query if r["run"] == run]
            run_rows.append({
                "run": run,
                "avg_latency_ms": sum(float(r["avg_latency_ms"]) for r in rows) / len(rows),
            })
        run_rows.sort(key=lambda r: int(r["run"].split(" seg", 1)[0]) if r["run"].split(" seg", 1)[0].isdigit() else 0)
        bar(run_rows, "run", "avg_latency_ms", "Average sharded latency by run", "ms", "sharded_latency_by_run.png")

mmap = read_csv("wiki_mmap_vs_memory.csv")
if mmap:
    labels = [r["operator_type"] for r in mmap]
    mem = [float(r["memory_latency_ms"]) for r in mmap]
    mm = [float(r["mmap_latency_ms"]) for r in mmap]
    x = list(range(len(labels)))
    plt.figure(figsize=(12, 5))
    plt.bar([i - 0.2 for i in x], mem, width=0.4, label="memory")
    plt.bar([i + 0.2 for i in x], mm, width=0.4, label="mmap")
    plt.title("Memory vs mmap latency")
    plt.xlabel("query")
    plt.ylabel("ms")
    plt.xticks(x, labels, rotation=30, ha="right")
    plt.legend()
    save(GRAPHS / "mmap_vs_memory_latency.png")

ranking = read_csv("wiki_ranking_stats.csv")
if ranking:
    labels = [r["query"][:24] for r in ranking]
    boolean = [float(r["boolean_only_latency_ms"]) for r in ranking]
    tfidf = [float(r["tfidf_latency_ms"]) for r in ranking]
    bm25 = [float(r["bm25_latency_ms"]) for r in ranking]
    x = list(range(len(labels)))
    plt.figure(figsize=(12, 5))
    plt.plot(x, boolean, marker="o", label="boolean")
    plt.plot(x, tfidf, marker="o", label="tfidf")
    plt.plot(x, bm25, marker="o", label="bm25")
    plt.title("Ranking latency")
    plt.xlabel("query")
    plt.ylabel("ms")
    plt.xticks(x, labels, rotation=30, ha="right")
    plt.legend()
    save(GRAPHS / "ranking_latency.png")

if created:
    print("[graphs] created:")
    for path in created:
        print(f"  {path}")
else:
    print("[graphs] no graphs created; run wiki benchmark first")
