package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"hw5/internal/benchdata"
	"hw5/internal/index"
	"hw5/internal/query"
	"hw5/internal/scoring"
	"hw5/internal/storage"
)

type Runner struct {
	CorpusName string
	Docs       int
	WikiPath   string
	IndexPath  string
	ResultsDir string
	Queries    []benchdata.QuerySpec
}

func NewRunner(wikiPath string, docs int) Runner {
	if docs <= 0 {
		docs = 5000
	}
	return Runner{
		CorpusName: "wiki",
		Docs:       docs,
		WikiPath:   wikiPath,
		IndexPath:  "./data/wiki.seg",
		ResultsDir: "../reports/hw5/results",
	}
}

func (r Runner) LoadDocs() ([]index.Document, error) {
	if r.WikiPath == "" {
		r.WikiPath = "./data/wiki_sample.jsonl"
	}
	if _, err := os.Stat(r.WikiPath); err != nil {
		return nil, fmt.Errorf("wiki JSONL not found: %s\nprepare it with: ../scripts/hw5_prepare_wiki_sample.sh DOWNLOAD=1 TARGET_GB=6\nexpected JSONL line: {\"id\":1,\"title\":\"Article title\",\"text\":\"Article text\"}", r.WikiPath)
	}
	return benchdata.ReadWikiJSONL(r.WikiPath, r.Docs)
}

func (r Runner) BuildIndexAndSegment(docs []index.Document) (*index.MemoryIndex, BuildStats, error) {
	start := time.Now()
	idx := benchdata.BuildIndex(docs)
	buildElapsed := time.Since(start)
	writeStart := time.Now()
	if err := storage.Write(idx, r.IndexPath); err != nil {
		return nil, BuildStats{}, err
	}
	writeElapsed := time.Since(writeStart)
	total := time.Since(start)
	st, _ := os.Stat(r.IndexPath)
	corpus := CorpusStatsFromIndex(r.CorpusName, idx)
	stats := BuildStats{
		CorpusName:           r.CorpusName,
		Docs:                 len(docs),
		TotalBuildTimeMS:     ms(buildElapsed),
		SegmentWriteTimeMS:   ms(writeElapsed),
		TotalTimeMS:          ms(total),
		UniqueTerms:          len(idx.Postings),
		TotalPostings:        corpus.TotalPostings,
		SegmentFileSizeBytes: 0,
	}
	if st != nil {
		stats.SegmentFileSizeBytes = st.Size()
	}
	if total.Seconds() > 0 {
		stats.DocsPerSec = float64(len(docs)) / total.Seconds()
		stats.TokensPerSec = float64(corpus.TotalTokens) / total.Seconds()
	}
	return idx, stats, nil
}

func CorpusStatsFromIndex(corpusName string, idx *index.MemoryIndex) CorpusStats {
	stats := CorpusStats{CorpusName: corpusName, Docs: idx.DocCount, UniqueTerms: len(idx.Postings), VocabularySize: len(idx.Postings)}
	stats.MinDocLen = int(^uint(0) >> 1)
	for _, l := range idx.DocLens {
		stats.TotalTokens += l
		if l < stats.MinDocLen {
			stats.MinDocLen = l
		}
		if l > stats.MaxDocLen {
			stats.MaxDocLen = l
		}
	}
	if stats.Docs > 0 {
		stats.AvgDocLen = float64(stats.TotalTokens) / float64(stats.Docs)
	}
	if stats.MinDocLen == int(^uint(0)>>1) {
		stats.MinDocLen = 0
	}
	for _, pl := range idx.Postings {
		stats.TotalPostings += len(pl.Postings)
		stats.TotalPositions += pl.TTF
	}
	stats.Top10TermsByDF = topTerms(idx, "df")
	stats.Top10TermsByTTF = topTerms(idx, "ttf")
	return stats
}

func (r Runner) PrepareQuerySuite(idx *index.MemoryIndex, docs []index.Document) ([]benchdata.QuerySpec, error) {
	return r.PrepareQuerySuiteWithPerType(idx, docs, 10)
}

func (r Runner) PrepareQuerySuiteWithPerType(idx *index.MemoryIndex, docs []index.Document, perType int) ([]benchdata.QuerySpec, error) {
	specs := benchdata.GenerateQuerySuite(idx, docs, perType)
	return specs, benchdata.WriteQuerySuite(benchdata.DefaultWikiQueryJSON, benchdata.DefaultWikiQueryTXT, specs)
}

func (r Runner) LoadOrPrepareQueries(idx *index.MemoryIndex, docs []index.Document) ([]benchdata.QuerySpec, error) {
	if specs, err := benchdata.ReadQuerySuite(benchdata.DefaultWikiQueryJSON); err == nil && len(specs) > 0 {
		return specs, nil
	}
	return r.PrepareQuerySuite(idx, docs)
}

func (r Runner) BenchQueries(idx *index.MemoryIndex, specs []benchdata.QuerySpec, backend string, rank scoring.RankMode, iterations int) ([]QueryLatency, error) {
	if iterations <= 0 {
		iterations = 5
	}
	var reader *storage.MmapSegmentReader
	var err error
	if backend == "mmap" {
		reader, err = storage.Open(r.IndexPath)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
	}
	rows := make([]QueryLatency, 0, len(specs))
	for _, spec := range specs {
		row, err := r.measureQuery(idx, reader, spec, backend, rank, iterations)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r Runner) BenchRanking(idx *index.MemoryIndex, specs []benchdata.QuerySpec, topK int, iterations int) ([]RankingStats, error) {
	if iterations <= 0 {
		iterations = 5
	}
	var rows []RankingStats
	for _, spec := range specs {
		if spec.OperatorType != "TERM" && spec.OperatorType != "AND" && spec.OperatorType != "BM25_TOPK" && spec.OperatorType != "TFIDF_TOPK" {
			continue
		}
		node, err := query.Parse(spec.Query)
		if err != nil {
			return nil, err
		}
		if _, err := query.Execute(idx, node); err != nil {
			return nil, err
		}
		var pl index.PostingList
		var booleanSamples, tfidfSamples, bm25Samples []float64
		for i := 0; i < iterations; i++ {
			start := time.Now()
			pl, err = query.Execute(idx, node)
			if err != nil {
				return nil, err
			}
			booleanSamples = append(booleanSamples, ms(time.Since(start)))

			start = time.Now()
			_ = scoring.TopK(idx, pl, node.QueryTerms(), scoring.RankTFIDF, topK)
			tfidfSamples = append(tfidfSamples, ms(time.Since(start)))

			start = time.Now()
			_ = scoring.TopK(idx, pl, node.QueryTerms(), scoring.RankBM25, topK)
			bm25Samples = append(bm25Samples, ms(time.Since(start)))
		}
		tfidf := scoring.TopK(idx, pl, node.QueryTerms(), scoring.RankTFIDF, topK)
		bm25 := scoring.TopK(idx, pl, node.QueryTerms(), scoring.RankBM25, topK)
		booleanMS := mean(booleanSamples)
		tfidfMS := mean(tfidfSamples)
		bm25MS := mean(bm25Samples)
		booleanLow, booleanHigh := ci95(booleanSamples)
		tfidfLow, tfidfHigh := ci95(tfidfSamples)
		bm25Low, bm25High := ci95(bm25Samples)
		row := RankingStats{Query: spec.Query, Hits: len(pl.Postings), TopK: topK, BooleanOnlyLatencyMS: booleanMS, TFIDFLatencyMS: tfidfMS, BM25LatencyMS: bm25MS}
		row.BooleanCILow = booleanLow
		row.BooleanCIHigh = booleanHigh
		row.TFIDFCILow = tfidfLow
		row.TFIDFCIHigh = tfidfHigh
		row.BM25CILow = bm25Low
		row.BM25CIHigh = bm25High
		if booleanMS > 0 {
			row.TFIDFOverheadPercent = 100 * tfidfMS / booleanMS
			row.BM25OverheadPercent = 100 * bm25MS / booleanMS
		}
		if len(tfidf) > 0 {
			row.Top1DocIDTFIDF = tfidf[0].DocID
			row.Top1TitleTFIDF = tfidf[0].Title
		}
		if len(bm25) > 0 {
			row.Top1DocIDBM25 = bm25[0].DocID
			row.Top1TitleBM25 = bm25[0].Title
		}
		rows = append(rows, row)
		if len(rows) >= 30 {
			break
		}
	}
	return rows, nil
}

func (r Runner) measureQuery(idx *index.MemoryIndex, reader *storage.MmapSegmentReader, spec benchdata.QuerySpec, backend string, rank scoring.RankMode, iterations int) (QueryLatency, error) {
	durations := make([]float64, 0, iterations)
	hits := 0
	var before, after runtime.MemStats
	if _, err := r.executeOnce(idx, reader, spec.Query, backend, rank); err != nil {
		return QueryLatency{}, err
	}
	runtime.ReadMemStats(&before)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		count, err := r.executeOnce(idx, reader, spec.Query, backend, rank)
		if err != nil {
			return QueryLatency{}, err
		}
		hits = count
		durations = append(durations, ms(time.Since(start)))
	}
	runtime.ReadMemStats(&after)
	sort.Float64s(durations)
	avg := mean(durations)
	avgLow, avgHigh := ci95(durations)
	qps := 0.0
	if avg > 0 {
		qps = 1000 / avg
	}
	return QueryLatency{
		CorpusName:         r.CorpusName,
		Docs:               idx.DocCount,
		Query:              spec.Query,
		OperatorType:       spec.OperatorType,
		RankMode:           string(rank),
		Backend:            backend,
		Iterations:         iterations,
		Hits:               hits,
		AvgLatencyMS:       avg,
		AvgLatencyCILow:    avgLow,
		AvgLatencyCIHigh:   avgHigh,
		QPS:                qps,
		AllocsPerQuery:     (after.Mallocs - before.Mallocs) / uint64(iterations),
	}, nil
}

func (r Runner) executeOnce(idx *index.MemoryIndex, reader *storage.MmapSegmentReader, rawQuery string, backend string, rank scoring.RankMode) (int, error) {
	node, err := query.Parse(rawQuery)
	if err != nil {
		return 0, err
	}
	active := idx
	if backend == "mmap" {
		if reader == nil {
			return 0, fmt.Errorf("mmap backend requires opened segment reader")
		}
		active, err = reader.Materialize(node.QueryTerms())
		if err != nil {
			return 0, err
		}
	}
	pl, err := query.Execute(active, node)
	if err != nil {
		return 0, err
	}
	if rank != scoring.RankNone {
		_ = scoring.TopK(active, pl, node.QueryTerms(), rank, 10)
	}
	return len(pl.Postings), nil
}

func (r Runner) ResultPath(name string) string {
	return filepath.Join(r.ResultsDir, name)
}

func topTerms(idx *index.MemoryIndex, by string) string {
	type item struct {
		term string
		v    int
	}
	items := make([]item, 0, len(idx.TermStats))
	for term, stat := range idx.TermStats {
		v := stat.DF
		if by == "ttf" {
			v = stat.TTF
		}
		items = append(items, item{term: term, v: v})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].v == items[j].v {
			return items[i].term < items[j].term
		}
		return items[i].v > items[j].v
	})
	if len(items) > 10 {
		items = items[:10]
	}
	parts := make([]string, len(items))
	for i, it := range items {
		parts[i] = fmt.Sprintf("%s:%d", it.term, it.v)
	}
	return strings.Join(parts, ";")
}


func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
