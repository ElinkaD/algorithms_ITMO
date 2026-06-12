package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"hw5/internal/benchdata"
	labbench "hw5/internal/benchmark"
	"hw5/internal/index"
	"hw5/internal/query"
	"hw5/internal/scoring"
	"hw5/internal/storage"
)

func main() {
	mode := flag.String("mode", "demo", "demo|build|search|repl|bench-smoke|build-wiki|wiki-stats|prepare-wiki-queries|bench-query-wiki|compression-stats-wiki|compression-stats-synthetic|bench-mmap-vs-memory|bench-ranking-wiki|bench-build-wiki|bench-wiki")
	docs := flag.Int("docs", 5000, "number of synthetic documents")
	indexPath := flag.String("index", "./data/index.seg", "segment path")
	input := flag.String("input", "./data/wiki_sample.jsonl", "wiki JSONL input")
	limit := flag.Int("limit", 5000, "wiki limit")
	queryText := flag.String("query", "data AND engineer", "search query")
	topK := flag.Int("topK", 10, "top K results")
	rank := flag.String("rank", "bm25", "bm25|tfidf")
	iterations := flag.Int("iterations", 5, "benchmark iterations per query")
	flag.Parse()

	var err error
	switch *mode {
	case "demo":
		err = runDemo()
	case "build":
		err = runBuild(*docs, *indexPath)
	case "search":
		err = runSearch(*indexPath, *queryText, *topK, scoring.RankMode(*rank))
	case "repl":
		err = runREPL(*indexPath)
	case "bench-smoke":
		err = runBenchSmoke(*docs)
	case "build-wiki":
		err = runBuildWiki(*input, *limit, *indexPath)
	case "wiki-stats":
		err = runWikiStats(*input, *limit)
	case "prepare-wiki-queries":
		err = runPrepareWikiQueries(*input, *limit, *indexPath)
	case "bench-build-wiki":
		err = runBenchBuildWiki(*input, *limit, *indexPath)
	case "bench-query-wiki":
		err = runBenchQueryWiki(*input, *limit, *indexPath, *iterations)
	case "compression-stats-wiki":
		err = runCompressionStatsWiki(*input, *limit, *indexPath)
	case "compression-stats-synthetic":
		err = runCompressionStatsSynthetic(*docs)
	case "bench-mmap-vs-memory":
		err = runBenchMmapVsMemory(*input, *limit, *indexPath)
	case "bench-ranking-wiki":
		err = runBenchRankingWiki(*input, *limit, *indexPath, *topK)
	case "bench-wiki":
		err = runBenchWiki(*input, *limit, *indexPath, *iterations, *topK)
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runDemo() error {
	fmt.Println("[demo] building in-memory index")
	idx := benchdata.BuildIndex(benchdata.DemoDocuments())
	fmt.Printf("[demo] documents: %d\n", idx.DocCount)
	fmt.Printf("[demo] unique terms: %d\n", len(idx.Postings))
	path := "./data/demo.seg"
	fmt.Printf("[demo] writing segment: %s\n", path)
	if err := storage.Write(idx, path); err != nil {
		return err
	}
	st, _ := os.Stat(path)
	fmt.Printf("[demo] segment size: %s\n", humanBytes(st.Size()))
	fmt.Println("[demo] opening mmap segment")
	queries := []string{
		"data AND engineer",
		`"new york"`,
		"data NEAR/3 pipeline",
		"data AND NOT python",
	}
	for _, q := range queries {
		fmt.Println()
		fmt.Printf("Query: %s\n", q)
		fmt.Println("Top results:")
		results, err := searchDisk(path, q, 5, scoring.RankBM25)
		if err != nil {
			return err
		}
		printResults(results)
	}
	fmt.Println()
	fmt.Println("[demo] done")
	return nil
}

func runBuild(n int, path string) error {
	start := time.Now()
	idx := benchdata.BuildIndex(benchdata.GenerateDocuments(n))
	if err := storage.Write(idx, path); err != nil {
		return err
	}
	fmt.Printf("[build] docs=%d uniqueTerms=%d elapsed=%s index=%s\n", idx.DocCount, len(idx.Postings), time.Since(start), path)
	return nil
}

func runBuildWiki(input string, limit int, path string) error {
	docs, err := benchdata.ReadWikiJSONL(input, limit)
	if err != nil {
		return wikiInputError(input, err)
	}
	start := time.Now()
	idx := benchdata.BuildIndex(docs)
	if err := storage.Write(idx, path); err != nil {
		return err
	}
	manifest := filepath.Join(filepath.Dir(path), "manifest.json")
	_ = storage.WriteManifest(manifest, []string{filepath.Base(path)})
	fmt.Printf("[build-wiki] docs=%d uniqueTerms=%d elapsed=%s index=%s manifest=%s\n", idx.DocCount, len(idx.Postings), time.Since(start), path, manifest)
	return nil
}

func runWikiStats(input string, limit int) error {
	r := labbench.NewRunner(input, limit)
	docs, err := r.LoadDocs()
	if err != nil {
		return err
	}
	idx := benchdata.BuildIndex(docs)
	stats := labbench.CorpusStatsFromIndex("wiki", idx)
	path := r.ResultPath("wiki_corpus_stats.csv")
	if err := labbench.WriteCorpusStats(path, stats); err != nil {
		return err
	}
	fmt.Printf("[wiki-stats] written %s docs=%d tokens=%d uniqueTerms=%d\n", path, stats.Docs, stats.TotalTokens, stats.UniqueTerms)
	return nil
}

func runPrepareWikiQueries(input string, limit int, indexPath string) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	docs, err := r.LoadDocs()
	if err != nil {
		return err
	}
	idx := benchdata.BuildIndex(docs)
	specs, err := r.PrepareQuerySuite(idx, docs)
	if err != nil {
		return err
	}
	fmt.Printf("[prepare-wiki-queries] queries=%d json=%s txt=%s\n", len(specs), benchdata.DefaultWikiQueryJSON, benchdata.DefaultWikiQueryTXT)
	return nil
}

func runBenchBuildWiki(input string, limit int, indexPath string) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	docs, err := r.LoadDocs()
	if err != nil {
		return err
	}
	_, stats, err := r.BuildIndexAndSegment(docs)
	if err != nil {
		return err
	}
	path := r.ResultPath("wiki_build_stats.csv")
	if err := labbench.WriteBuildStats(path, stats); err != nil {
		return err
	}
	fmt.Printf("[bench-build-wiki] written %s total=%.1fms docsPerSec=%.1f\n", path, stats.TotalTimeMS, stats.DocsPerSec)
	return nil
}

func runBenchQueryWiki(input string, limit int, indexPath string, iterations int) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	docs, idx, err := ensureWikiIndex(r)
	if err != nil {
		return err
	}
	specs, err := r.LoadOrPrepareQueries(idx, docs)
	if err != nil {
		return err
	}
	rows, err := r.BenchQueries(idx, specs, "mmap", scoring.RankBM25, iterations)
	if err != nil {
		return err
	}
	path := r.ResultPath("wiki_query_latency.csv")
	if err := labbench.WriteQueryLatency(path, rows); err != nil {
		return err
	}
	operatorPath := r.ResultPath("wiki_operator_stats.csv")
	if err := labbench.WriteQueryLatency(operatorPath, rows); err != nil {
		return err
	}
	fmt.Printf("[bench-query-wiki] written %s rows=%d\n", path, len(rows))
	return nil
}

func runCompressionStatsWiki(input string, limit int, indexPath string) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	_, idx, err := ensureWikiIndex(r)
	if err != nil {
		return err
	}
	stats, err := storage.ComputeSizeStats("wiki", idx, indexPath)
	if err != nil {
		return err
	}
	path := r.ResultPath("wiki_compression_stats.csv")
	if err := storage.WriteCompressionCSV(path, []storage.SegmentSizeStats{stats}); err != nil {
		return err
	}
	fmt.Printf("[compression-stats-wiki] written %s ratio=%.3f saving=%.2f%%\n", path, stats.CompressionRatio, stats.SpaceSavingPercent)
	return nil
}

func runCompressionStatsSynthetic(docs int) error {
	path := "../reports/hw5/results/synthetic_compression_stats.csv"
	stats, err := labbench.SyntheticCompressionStats(docs, "./data/synthetic_compression.seg")
	if err != nil {
		return err
	}
	if err := storage.WriteCompressionCSV(path, []storage.SegmentSizeStats{stats}); err != nil {
		return err
	}
	fmt.Printf("[compression-stats-synthetic] written %s ratio=%.3f\n", path, stats.CompressionRatio)
	return nil
}

func runBenchMmapVsMemory(input string, limit int, indexPath string) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	docs, idx, err := ensureWikiIndex(r)
	if err != nil {
		return err
	}
	specs, err := r.LoadOrPrepareQueries(idx, docs)
	if err != nil {
		return err
	}
	rows, err := r.BenchMmapVsMemory(idx, specs)
	if err != nil {
		return err
	}
	path := r.ResultPath("wiki_mmap_vs_memory.csv")
	if err := labbench.WriteMmapVsMemory(path, rows); err != nil {
		return err
	}
	fmt.Printf("[bench-mmap-vs-memory] written %s rows=%d\n", path, len(rows))
	return nil
}

func runBenchRankingWiki(input string, limit int, indexPath string, topK int) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	docs, idx, err := ensureWikiIndex(r)
	if err != nil {
		return err
	}
	specs, err := r.LoadOrPrepareQueries(idx, docs)
	if err != nil {
		return err
	}
	rows, err := r.BenchRanking(idx, specs, topK)
	if err != nil {
		return err
	}
	path := r.ResultPath("wiki_ranking_stats.csv")
	if err := labbench.WriteRankingStats(path, rows); err != nil {
		return err
	}
	fmt.Printf("[bench-ranking-wiki] written %s rows=%d\n", path, len(rows))
	return nil
}

func runBenchWiki(input string, limit int, indexPath string, iterations int, topK int) error {
	if err := runWikiStats(input, limit); err != nil {
		return err
	}
	if err := runBenchBuildWiki(input, limit, indexPath); err != nil {
		return err
	}
	if err := runPrepareWikiQueries(input, limit, indexPath); err != nil {
		return err
	}
	if err := runCompressionStatsWiki(input, limit, indexPath); err != nil {
		return err
	}
	if err := runBenchQueryWiki(input, limit, indexPath, iterations); err != nil {
		return err
	}
	if err := runBenchMmapVsMemory(input, limit, indexPath); err != nil {
		return err
	}
	return runBenchRankingWiki(input, limit, indexPath, topK)
}

func runSearch(path, q string, topK int, mode scoring.RankMode) error {
	results, err := searchDisk(path, q, topK, mode)
	if err != nil {
		return err
	}
	fmt.Printf("Query: %s\n", q)
	fmt.Println("Top results:")
	printResults(results)
	return nil
}

func runREPL(path string) error {
	topK := 10
	mode := scoring.RankBM25
	fmt.Println("[repl] :help for commands")
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("hw5-search> ")
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, ":") {
			switch {
			case line == ":help":
				fmt.Println(":stats | :mode bm25 | :mode tfidf | :mode none | :topk 10 | :explain query | :exit")
			case line == ":exit":
				return nil
			case line == ":stats":
				r, err := storage.Open(path)
				if err != nil {
					fmt.Println("error:", err)
					continue
				}
				fmt.Printf("%+v\n", r.Stats())
				_ = r.Close()
			case strings.HasPrefix(line, ":mode "):
				next := scoring.RankMode(strings.TrimSpace(strings.TrimPrefix(line, ":mode ")))
				if next == scoring.RankBM25 || next == scoring.RankTFIDF || next == scoring.RankNone {
					mode = next
					fmt.Println("[repl] rank mode:", mode)
				}
			case strings.HasPrefix(line, ":topk "):
				_, _ = fmt.Sscanf(line, ":topk %d", &topK)
				fmt.Println("[repl] topK:", topK)
			case strings.HasPrefix(line, ":explain "):
				if err := explainQuery(path, strings.TrimSpace(strings.TrimPrefix(line, ":explain ")), topK, mode); err != nil {
					fmt.Println("error:", err)
				}
			default:
				fmt.Println("unknown command")
			}
			continue
		}
		results, err := searchDisk(path, line, topK, mode)
		if err != nil {
			fmt.Println("error:", err)
			continue
		}
		printResults(results)
	}
	return sc.Err()
}

func runBenchSmoke(n int) error {
	fmt.Printf("[bench-smoke] docs=%d\n", n)
	docs := benchdata.GenerateDocuments(n)
	start := time.Now()
	idx := benchdata.BuildIndex(docs)
	buildTime := time.Since(start)
	path := "./data/bench_smoke.seg"
	rawSize := idx.RawPostingsSize()
	if err := storage.Write(idx, path); err != nil {
		return err
	}
	r, err := storage.Open(path)
	if err != nil {
		return err
	}
	defer r.Close()
	stats := r.Stats()
	compressedPostings := stats.DocNormsOffset - stats.PostingsOffset
	fmt.Printf("[bench-smoke] uniqueTerms=%d\n", len(idx.Postings))
	fmt.Printf("[bench-smoke] buildTime=%s\n", buildTime)
	fmt.Printf("[bench-smoke] rawPostingsSize=%s\n", humanBytes(rawSize))
	fmt.Printf("[bench-smoke] compressedPostingsSize=%s\n", humanBytes(compressedPostings))
	if rawSize > 0 {
		fmt.Printf("[bench-smoke] compressionRatio=%.2f\n", float64(compressedPostings)/float64(rawSize))
	}
	queries := []string{"term42 AND term77", "term42 OR term77", "alpha ADJ beta", "alpha NEAR/5 beta", "data NEAR/5 pipeline"}
	for _, q := range queries {
		start = time.Now()
		results, hits, err := searchDiskWithHits(path, q, 10, scoring.RankBM25)
		if err != nil {
			return err
		}
		_ = results
		fmt.Printf("[bench-smoke] query=%q hits=%d latency=%s\n", q, hits, time.Since(start))
	}
	fmt.Println("[bench-smoke] done")
	return nil
}

func searchDisk(path, rawQuery string, topK int, mode scoring.RankMode) ([]scoring.SearchResult, error) {
	results, _, err := searchDiskWithHits(path, rawQuery, topK, mode)
	return results, err
}

func searchDiskWithHits(path, rawQuery string, topK int, mode scoring.RankMode) ([]scoring.SearchResult, int, error) {
	node, err := query.Parse(rawQuery)
	if err != nil {
		return nil, 0, err
	}
	r, err := storage.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()
	idx, err := r.Materialize(node.QueryTerms())
	if err != nil {
		return nil, 0, err
	}
	pl, err := query.Execute(idx, node)
	if err != nil {
		return nil, 0, err
	}
	return scoring.TopK(idx, pl, node.QueryTerms(), mode, topK), len(pl.Postings), nil
}

func explainQuery(path, rawQuery string, topK int, mode scoring.RankMode) error {
	start := time.Now()
	node, err := query.Parse(rawQuery)
	if err != nil {
		return err
	}
	r, err := storage.Open(path)
	if err != nil {
		return err
	}
	defer r.Close()
	idx, err := r.Materialize(node.QueryTerms())
	if err != nil {
		return err
	}
	pl, err := query.Execute(idx, node)
	if err != nil {
		return err
	}
	results := scoring.TopK(idx, pl, node.QueryTerms(), mode, topK)
	fmt.Printf("parsed AST: %#v\n", node)
	fmt.Printf("backend: mmap\n")
	fmt.Printf("rank mode: %s\n", mode)
	fmt.Printf("terms: %v\n", node.QueryTerms())
	for _, term := range node.QueryTerms() {
		fmt.Printf("df[%s]=%d\n", term, idx.DF(term))
	}
	fmt.Printf("hits=%d latency=%s\n", len(pl.Postings), time.Since(start))
	printResults(results)
	return nil
}

func printResults(results []scoring.SearchResult) {
	if len(results) == 0 {
		fmt.Println("No results")
		return
	}
	for i, r := range results {
		fmt.Printf("%d. docId=%d score=%.3f title=%q\n", i+1, r.DocID, r.Score, r.Title)
	}
}

func humanBytes(n int64) string {
	units := []string{"B", "KB", "MB", "GB"}
	v := float64(n)
	u := 0
	for v >= 1024 && u < len(units)-1 {
		v /= 1024
		u++
	}
	if u == 0 {
		return fmt.Sprintf("%d %s", n, units[u])
	}
	return fmt.Sprintf("%.1f %s", v, units[u])
}

func ensureWikiIndex(r labbench.Runner) ([]index.Document, *index.MemoryIndex, error) {
	docs, err := r.LoadDocs()
	if err != nil {
		return nil, nil, err
	}
	idx := benchdata.BuildIndex(docs)
	if _, err := os.Stat(r.IndexPath); err != nil {
		if err := storage.Write(idx, r.IndexPath); err != nil {
			return nil, nil, err
		}
	}
	return docs, idx, nil
}

func wikiInputError(input string, err error) error {
	return fmt.Errorf("wiki JSONL is not available: %s: %w\nprepare it with: ../scripts/hw5_prepare_wiki_sample.sh DOWNLOAD=1 TARGET_GB=6\nor put JSONL lines like {\"id\":1,\"title\":\"Article title\",\"text\":\"Article text\"} into %s", input, err, input)
}
