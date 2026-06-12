#!/usr/bin/env python3
from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "reports" / "hw5" / "results"
GRAPHS = ROOT / "graphs" / "hw5"
GRAPHS.mkdir(parents=True, exist_ok=True)

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


wiki_comp = read_csv("wiki_compression_stats.csv")
synthetic_comp = read_csv("synthetic_compression_stats.csv")
comp = synthetic_comp + wiki_comp
if comp:
    bar(comp, "corpus_name", "compression_ratio", "Compression ratio by corpus", "raw / compressed", "compression_ratio_by_corpus.png")
    labels = [f"{r.get('corpus_name', 'corpus')}:{r.get('docs', '')}" for r in comp]
    raw = [float(r["raw_total_bytes"]) for r in comp if r.get("raw_total_bytes")]
    compressed = [float(r["compressed_total_bytes"]) for r in comp if r.get("compressed_total_bytes")]
    if raw and compressed and len(raw) == len(compressed):
        x = list(range(len(labels)))
        plt.figure(figsize=(10, 5))
        plt.bar([i - 0.2 for i in x], raw, width=0.4, label="raw")
        plt.bar([i + 0.2 for i in x], compressed, width=0.4, label="compressed")
        plt.title("Raw vs compressed size")
        plt.xlabel("corpus")
        plt.ylabel("bytes")
        plt.xticks(x, labels, rotation=30, ha="right")
        plt.legend()
        save(GRAPHS / "raw_vs_compressed_size.png")

build = read_csv("wiki_build_stats.csv")
bar(build, "docs", "total_time_ms", "Build time by docs", "ms", "build_time_by_docs.png")

query = read_csv("wiki_query_latency.csv")
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
