package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"hw5/internal/benchdata"
	labbench "hw5/internal/benchmark"
	"hw5/internal/index"
	"hw5/internal/query"
	"hw5/internal/scoring"
	"hw5/internal/storage"
)

func main() {
	mode := flag.String("mode", "demo", "demo|build|search|repl|bench-smoke|build-wiki|build-wiki-shards|wiki-stats|wiki-scale-stats|wiki-report-scale-stats|prepare-wiki-queries|bench-query-wiki|bench-query-wiki-shards|compression-stats-wiki|compression-stats-synthetic|bench-mmap-vs-memory|bench-ranking-wiki|bench-build-wiki|bench-wiki")
	docs := flag.Int("docs", 5000, "number of synthetic documents")
	indexPath := flag.String("index", "./data/index.seg", "segment path")
	halfIndexPath := flag.String("half-index", "./data/wiki_shards/prefix-417728", "half-dataset shard directory")
	input := flag.String("input", "./data/wiki_sample.jsonl", "wiki JSONL input")
	limit := flag.Int("limit", 5000, "wiki limit")
	halfLimit := flag.Int("half-limit", 417728, "half-dataset wiki limit")
	queryText := flag.String("query", "data AND engineer", "search query")
	topK := flag.Int("topK", 10, "top K results")
	rank := flag.String("rank", "bm25", "bm25|tfidf")
	iterations := flag.Int("iterations", 5, "benchmark iterations per query")
	segmentDocs := flag.Int("segment-docs", 50000, "documents per shard segment")
	shardLimit := flag.Int("shard-limit", 0, "limit number of shard segments to open; 0 means all")
	queriesPerType := flag.Int("queries-per-type", 10, "generated query count per operator type")
	querySampleDocs := flag.Int("query-sample-docs", 10000, "wiki documents used to generate sharded query suite")
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
	case "build-wiki-shards":
		err = runBuildWikiShards(*input, *limit, *indexPath, *segmentDocs)
	case "wiki-stats":
		err = runWikiStats(*input, *limit)
	case "wiki-scale-stats":
		err = runWikiScaleStats(*input, *limit, *indexPath, *segmentDocs)
	case "wiki-report-scale-stats":
		err = runWikiReportScaleStats(*input, *limit, *indexPath, *halfLimit, *halfIndexPath, *segmentDocs)
	case "prepare-wiki-queries":
		err = runPrepareWikiQueries(*input, *limit, *indexPath, *queriesPerType)
	case "bench-build-wiki":
		err = runBenchBuildWiki(*input, *limit, *indexPath, *iterations)
	case "bench-query-wiki":
		err = runBenchQueryWiki(*input, *limit, *indexPath, *iterations)
	case "bench-query-wiki-shards":
		err = runBenchQueryWikiShards(*input, *limit, *indexPath, *segmentDocs, *shardLimit, *queriesPerType, *querySampleDocs, *iterations)
	case "compression-stats-wiki":
		err = runCompressionStatsWiki(*input, *limit, *indexPath)
	case "compression-stats-synthetic":
		err = runCompressionStatsSynthetic(*docs)
	case "bench-mmap-vs-memory":
		err = runBenchMmapVsMemory(*input, *limit, *indexPath, *iterations)
	case "bench-ranking-wiki":
		err = runBenchRankingWiki(*input, *limit, *indexPath, *topK, *iterations)
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

func runBuildWikiShards(input string, limit int, indexDir string, segmentDocs int) error {
	if segmentDocs <= 0 {
		segmentDocs = 50000
	}
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return err
	}
	start := time.Now()
	var segments []string
	totalDocs := 0
	err := benchdata.ReadWikiJSONLChunks(input, limit, segmentDocs, func(chunkIndex int, docs []index.Document) error {
		chunkStart := time.Now()
		idx := benchdata.BuildIndex(docs)
		name := fmt.Sprintf("shard-%05d.seg", chunkIndex)
		path := filepath.Join(indexDir, name)
		if err := storage.Write(idx, path); err != nil {
			return err
		}
		segments = append(segments, name)
		totalDocs += len(docs)
		fmt.Printf("[build-wiki-shards] shard=%d docs=%d uniqueTerms=%d elapsed=%s path=%s\n", chunkIndex, len(docs), len(idx.Postings), time.Since(chunkStart), path)
		return nil
	})
	if err != nil {
		return wikiInputError(input, err)
	}
	manifest := filepath.Join(indexDir, "manifest.json")
	if err := storage.WriteManifest(manifest, segments); err != nil {
		return err
	}
	fmt.Printf("[build-wiki-shards] docs=%d shards=%d segmentDocs=%d elapsed=%s manifest=%s\n", totalDocs, len(segments), segmentDocs, time.Since(start), manifest)
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

func runWikiScaleStats(input string, limit int, indexDir string, segmentDocs int) error {
	rows, err := collectScaleStats(input, limit, indexDir, segmentDocs, nil)
	if err != nil {
		return err
	}
	path := filepath.Join("../reports/hw5/results", "wiki_scale_stats.csv")
	if err := labbench.WriteScaleStats(path, rows); err != nil {
		return err
	}
	fmt.Printf("[wiki-scale-stats] written %s rows=%d\n", path, len(rows))
	return nil
}

func runWikiReportScaleStats(input string, fullLimit int, fullIndexDir string, halfLimit int, halfIndexDir string, segmentDocs int) error {
	oneRows, err := collectScaleStats(input, segmentDocs, fullIndexDir, segmentDocs, map[int]bool{1: true})
	if err != nil {
		return err
	}
	halfRows, err := collectScaleStats(input, halfLimit, halfIndexDir, segmentDocs, map[int]bool{(halfLimit + segmentDocs - 1) / segmentDocs: true})
	if err != nil {
		return err
	}
	fullRows, err := collectScaleStats(input, fullLimit, fullIndexDir, segmentDocs, map[int]bool{(fullLimit + segmentDocs - 1) / segmentDocs: true})
	if err != nil {
		return err
	}
	rows := make([]labbench.ScaleStats, 0, len(oneRows)+len(halfRows)+len(fullRows))
	rows = append(rows, oneRows...)
	rows = append(rows, halfRows...)
	rows = append(rows, fullRows...)
	path := filepath.Join("../reports/hw5/results", "wiki_scale_stats.csv")
	if err := labbench.WriteScaleStats(path, rows); err != nil {
		return err
	}
	fmt.Printf("[wiki-report-scale-stats] written %s rows=%d\n", path, len(rows))
	return nil
}

func collectScaleStats(input string, limit int, indexDir string, segmentDocs int, milestoneOverride map[int]bool) ([]labbench.ScaleStats, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	if segmentDocs <= 0 {
		segmentDocs = 50000
	}
	totalShards := (limit + segmentDocs - 1) / segmentDocs
	halfShards := (totalShards + 1) / 2
	milestones := map[int]bool{1: true, halfShards: true, totalShards: true}
	if milestoneOverride != nil {
		milestones = milestoneOverride
	}

	rows := make([]labbench.ScaleStats, 0, 3)
	cumulative := labbench.ScaleStats{
		CorpusName: "wiki",
		MinDocLen:  int(^uint(0) >> 1),
	}
	currentShard := 0
	err := benchdata.ReadWikiJSONLChunks(input, limit, segmentDocs, func(chunkIndex int, docs []index.Document) error {
		currentShard = chunkIndex + 1
		idx := benchdata.BuildIndex(docs)
		corpus := labbench.CorpusStatsFromIndex("wiki", idx)
		segmentPath := filepath.Join(indexDir, fmt.Sprintf("shard-%05d.seg", chunkIndex))
		sizeStats, err := storage.ComputeSizeStats("wiki", idx, segmentPath)
		if err != nil {
			return err
		}

		cumulative.Docs += corpus.Docs
		cumulative.Shards = currentShard
		cumulative.TotalTokens += corpus.TotalTokens
		cumulative.TotalPostings += corpus.TotalPostings
		cumulative.TotalPositions += corpus.TotalPositions
		cumulative.RawTotalBytes += sizeStats.RawTotalBytes
		cumulative.CompressedTotalBytes += sizeStats.CompressedTotalBytes
		cumulative.SegmentFileSizeBytes += sizeStats.SegmentFileSizeBytes
		if corpus.MinDocLen < cumulative.MinDocLen {
			cumulative.MinDocLen = corpus.MinDocLen
		}
		if corpus.MaxDocLen > cumulative.MaxDocLen {
			cumulative.MaxDocLen = corpus.MaxDocLen
		}
		if cumulative.Docs > 0 {
			cumulative.AvgDocLen = float64(cumulative.TotalTokens) / float64(cumulative.Docs)
		}
		if cumulative.RawTotalBytes > 0 && cumulative.CompressedTotalBytes > 0 {
			cumulative.CompressionRatio = float64(cumulative.RawTotalBytes) / float64(cumulative.CompressedTotalBytes)
			cumulative.SpaceSavingPercent = 100 * (1 - float64(cumulative.CompressedTotalBytes)/float64(cumulative.RawTotalBytes))
		}
		if milestones[currentShard] {
			snapshot := cumulative
			rows = append(rows, snapshot)
			fmt.Printf("[wiki-scale-stats] milestone shard=%d docs=%d raw=%.2fMB compressed=%.2fMB segment=%.2fMB\n",
				currentShard,
				snapshot.Docs,
				float64(snapshot.RawTotalBytes)/1_000_000,
				float64(snapshot.CompressedTotalBytes)/1_000_000,
				float64(snapshot.SegmentFileSizeBytes)/1_000_000,
			)
		}
		return nil
	})
	if err != nil {
		return nil, wikiInputError(input, err)
	}
	if cumulative.MinDocLen == int(^uint(0)>>1) {
		cumulative.MinDocLen = 0
	}
	return rows, nil
}

func runPrepareWikiQueries(input string, limit int, indexPath string, queriesPerType int) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	docs, err := r.LoadDocs()
	if err != nil {
		return err
	}
	idx := benchdata.BuildIndex(docs)
	specs, err := r.PrepareQuerySuiteWithPerType(idx, docs, queriesPerType)
	if err != nil {
		return err
	}
	fmt.Printf("[prepare-wiki-queries] queries=%d json=%s txt=%s\n", len(specs), benchdata.DefaultWikiQueryJSON, benchdata.DefaultWikiQueryTXT)
	return nil
}

func runBenchBuildWiki(input string, limit int, indexPath string, iterations int) error {
	r := labbench.NewRunner(input, limit)
	r.IndexPath = indexPath
	docs, err := r.LoadDocs()
	if err != nil {
		return err
	}
	if iterations <= 0 {
		iterations = 5
	}
	buildSamples := make([]float64, 0, iterations)
	writeSamples := make([]float64, 0, iterations)
	totalSamples := make([]float64, 0, iterations)
	var stats labbench.BuildStats
	for i := 0; i < iterations; i++ {
		_, stats, err = r.BuildIndexAndSegment(docs)
		if err != nil {
			return err
		}
		buildSamples = append(buildSamples, stats.TotalBuildTimeMS)
		writeSamples = append(writeSamples, stats.SegmentWriteTimeMS)
		totalSamples = append(totalSamples, stats.TotalTimeMS)
	}
	stats.TotalBuildTimeMS = labbench.Mean(buildSamples)
	stats.SegmentWriteTimeMS = labbench.Mean(writeSamples)
	stats.TotalTimeMS = labbench.Mean(totalSamples)
	stats.TotalBuildTimeCILow, stats.TotalBuildTimeCIHigh = labbench.CI95(buildSamples)
	stats.SegmentWriteCILow, stats.SegmentWriteCIHigh = labbench.CI95(writeSamples)
	stats.TotalTimeCILow, stats.TotalTimeCIHigh = labbench.CI95(totalSamples)
	path := r.ResultPath("wiki_build_stats.csv")
	if err := labbench.WriteBuildStats(path, stats); err != nil {
		return err
	}
	fmt.Printf("[bench-build-wiki] written %s iterations=%d total=%.1fms ci=[%.1f, %.1f] docsPerSec=%.1f\n", path, iterations, stats.TotalTimeMS, stats.TotalTimeCILow, stats.TotalTimeCIHigh, stats.DocsPerSec)
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

func runBenchQueryWikiShards(input string, limit int, indexDir string, segmentDocs int, shardLimit int, queriesPerType int, querySampleDocs int, iterations int) error {
	if segmentDocs <= 0 {
		segmentDocs = 50000
	}
	if querySampleDocs <= 0 {
		querySampleDocs = 10000
	}
	if iterations <= 0 {
		iterations = 5
	}
	queryLimit := querySampleDocs
	if limit > 0 && limit < queryLimit {
		queryLimit = limit
	}
	fmt.Printf("[bench-query-wiki-shards] preparing queries docs=%d queriesPerType=%d\n", queryLimit, queriesPerType)
	queryDocs, err := benchdata.ReadWikiJSONL(input, queryLimit)
	if err != nil {
		return wikiInputError(input, err)
	}
	queryIdx := benchdata.BuildIndex(queryDocs)
	specs := benchdata.GenerateQuerySuite(queryIdx, queryDocs, queriesPerType)
	if err := benchdata.WriteQuerySuite(benchdata.DefaultWikiQueryJSON, benchdata.DefaultWikiQueryTXT, specs); err != nil {
		return err
	}
	fmt.Printf("[bench-query-wiki-shards] prepared queries=%d\n", len(specs))
	manifestPath := filepath.Join(indexDir, "manifest.json")
	manifest, err := storage.ReadManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("read shard manifest %s: %w", manifestPath, err)
	}
	segments := manifest.Segments
	if shardLimit > 0 && shardLimit < len(segments) {
		segments = segments[:shardLimit]
	}
	readers := make([]*storage.MmapSegmentReader, 0, len(segments))
	for _, segment := range segments {
		segmentPath := filepath.Join(indexDir, segment)
		reader, err := storage.Open(segmentPath)
		if err != nil {
			for _, r := range readers {
				_ = r.Close()
			}
			return err
		}
		readers = append(readers, reader)
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	rows := make([]labbench.QueryLatency, 0, len(specs))
	for i, spec := range specs {
		row, err := measureShardedQuery(readers, spec, iterations)
		if err != nil {
			return err
		}
		row.CorpusName = "wiki"
		row.Docs = limit
		row.Query = spec.Query
		row.OperatorType = spec.OperatorType
		row.RankMode = string(scoring.RankBM25)
		row.Backend = "mmap-shards"
		row.Iterations = iterations
		rows = append(rows, row)
		if (i+1)%100 == 0 || i+1 == len(specs) {
			fmt.Printf("[bench-query-wiki-shards] progress=%d/%d query=%q avg=%.3fms hits=%d\n", i+1, len(specs), spec.Query, row.AvgLatencyMS, row.Hits)
		}
	}
	path := filepath.Join("../reports/hw5/results", "wiki_sharded_query_latency.csv")
	if err := labbench.WriteQueryLatency(path, rows); err != nil {
		return err
	}
	snapshotPath := filepath.Join("../reports/hw5/results", fmt.Sprintf("wiki_sharded_query_latency_docs%d_shards%d.csv", limit, len(readers)))
	if err := labbench.WriteQueryLatency(snapshotPath, rows); err != nil {
		return err
	}
	fmt.Printf("[bench-query-wiki-shards] written %s rows=%d shards=%d shardLimit=%d queriesPerType=%d snapshot=%s\n", path, len(rows), len(readers), shardLimit, queriesPerType, snapshotPath)
	return nil
}

func measureShardedQuery(readers []*storage.MmapSegmentReader, spec benchdata.QuerySpec, iterations int) (labbench.QueryLatency, error) {
	var row labbench.QueryLatency
	if _, err := executeShardedOnce(readers, spec.Query); err != nil {
		return row, err
	}
	durations := make([]float64, 0, iterations)
	hits := 0
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		count, err := executeShardedOnce(readers, spec.Query)
		if err != nil {
			return row, err
		}
		hits = count
		durations = append(durations, float64(time.Since(start).Microseconds())/1000)
	}
	runtime.ReadMemStats(&after)
	sort.Float64s(durations)
	avg := mean(durations)
	avgLow, avgHigh := labbench.CI95(durations)
	qps := 0.0
	if avg > 0 {
		qps = 1000 / avg
	}
	qpsLow := 0.0
	qpsHigh := 0.0
	if avgHigh > 0 {
		qpsLow = 1000 / avgHigh
	}
	if avgLow > 0 {
		qpsHigh = 1000 / avgLow
	}
	row.Hits = hits
	row.AvgLatencyMS = avg
	row.AvgLatencyCILow = avgLow
	row.AvgLatencyCIHigh = avgHigh
	row.P50LatencyMS = quantile(durations, 0.50)
	row.P95LatencyMS = quantile(durations, 0.95)
	row.MinLatencyMS = durations[0]
	row.MaxLatencyMS = durations[len(durations)-1]
	row.QPS = qps
	row.QPSCILow = qpsLow
	row.QPSCIHigh = qpsHigh
	row.AllocBytesPerQuery = (after.TotalAlloc - before.TotalAlloc) / uint64(iterations)
	row.AllocsPerQuery = (after.Mallocs - before.Mallocs) / uint64(iterations)
	return row, nil
}

func executeShardedOnce(readers []*storage.MmapSegmentReader, rawQuery string) (int, error) {
	node, err := query.Parse(rawQuery)
	if err != nil {
		return 0, err
	}
	terms := node.QueryTerms()
	type result struct {
		hits int
		err  error
	}
	results := make(chan result, len(readers))
	var wg sync.WaitGroup
	for _, reader := range readers {
		wg.Add(1)
		go func(reader *storage.MmapSegmentReader) {
			defer wg.Done()
			idx, err := reader.Materialize(terms)
			if err != nil {
				results <- result{err: err}
				return
			}
			pl, err := query.Execute(idx, node)
			if err != nil {
				results <- result{err: err}
				return
			}
			_ = scoring.TopK(idx, pl, terms, scoring.RankBM25, 10)
			results <- result{hits: len(pl.Postings)}
		}(reader)
	}
	wg.Wait()
	close(results)
	total := 0
	for res := range results {
		if res.err != nil {
			return 0, res.err
		}
		total += res.hits
	}
	return total, nil
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

func runBenchMmapVsMemory(input string, limit int, indexPath string, iterations int) error {
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
	rows, err := r.BenchMmapVsMemory(idx, specs, iterations)
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

func runBenchRankingWiki(input string, limit int, indexPath string, topK int, iterations int) error {
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
	rows, err := r.BenchRanking(idx, specs, topK, iterations)
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
	if err := runBenchBuildWiki(input, limit, indexPath, iterations); err != nil {
		return err
	}
	if err := runPrepareWikiQueries(input, limit, indexPath, 10); err != nil {
		return err
	}
	if err := runCompressionStatsWiki(input, limit, indexPath); err != nil {
		return err
	}
	if err := runBenchQueryWiki(input, limit, indexPath, iterations); err != nil {
		return err
	}
	if err := runBenchMmapVsMemory(input, limit, indexPath, iterations); err != nil {
		return err
	}
	return runBenchRankingWiki(input, limit, indexPath, topK, iterations)
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

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func quantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	pos := int(float64(len(values)-1) * q)
	if pos < 0 {
		pos = 0
	}
	if pos >= len(values) {
		pos = len(values) - 1
	}
	return values[pos]
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
