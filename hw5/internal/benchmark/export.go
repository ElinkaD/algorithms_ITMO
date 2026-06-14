package benchmark

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

func writeCSV(path string, header []string, rows [][]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func WriteCorpusStats(path string, row CorpusStats) error {
	return writeCSV(path,
		[]string{"corpus_name", "docs", "total_tokens", "unique_terms", "avg_doc_len", "min_doc_len", "max_doc_len", "total_postings", "total_positions", "vocabulary_size", "top_10_terms_by_df", "top_10_terms_by_ttf"},
		[][]string{{
			row.CorpusName, itoa(row.Docs), itoa(row.TotalTokens), itoa(row.UniqueTerms),
			fmt.Sprintf("%.2f", row.AvgDocLen), itoa(row.MinDocLen), itoa(row.MaxDocLen),
			itoa(row.TotalPostings), itoa(row.TotalPositions), itoa(row.VocabularySize),
			row.Top10TermsByDF, row.Top10TermsByTTF,
		}})
}

func WriteBuildStats(path string, row BuildStats) error {
	return writeCSV(path,
		[]string{"corpus_name", "docs", "total_build_time_ms", "total_build_time_ci_low_ms", "total_build_time_ci_high_ms", "segment_write_time_ms", "segment_write_ci_low_ms", "segment_write_ci_high_ms", "total_time_ms", "total_time_ci_low_ms", "total_time_ci_high_ms", "docs_per_sec", "tokens_per_sec", "unique_terms", "total_postings", "segment_file_size_bytes"},
		[][]string{{
			row.CorpusName, itoa(row.Docs), f(row.TotalBuildTimeMS), f(row.TotalBuildTimeCILow), f(row.TotalBuildTimeCIHigh), f(row.SegmentWriteTimeMS), f(row.SegmentWriteCILow), f(row.SegmentWriteCIHigh),
			f(row.TotalTimeMS), f(row.TotalTimeCILow), f(row.TotalTimeCIHigh), f(row.DocsPerSec), f(row.TokensPerSec), itoa(row.UniqueTerms),
			itoa(row.TotalPostings), strconv.FormatInt(row.SegmentFileSizeBytes, 10),
		}})
}

func WriteQueryLatency(path string, rows []QueryLatency) error {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			row.CorpusName, itoa(row.Docs), row.Query, row.OperatorType, row.RankMode, row.Backend,
			itoa(row.Iterations), itoa(row.Hits), f(row.AvgLatencyMS), f(row.AvgLatencyCILow), f(row.AvgLatencyCIHigh), f(row.P50LatencyMS),
			f(row.P95LatencyMS), f(row.MinLatencyMS), f(row.MaxLatencyMS), f(row.QPS), f(row.QPSCILow), f(row.QPSCIHigh),
			strconv.FormatUint(row.AllocBytesPerQuery, 10), strconv.FormatUint(row.AllocsPerQuery, 10),
		})
	}
	return writeCSV(path,
		[]string{"corpus_name", "docs", "query", "operator_type", "rank_mode", "backend", "iterations", "hits", "avg_latency_ms", "avg_latency_ci_low_ms", "avg_latency_ci_high_ms", "p50_latency_ms", "p95_latency_ms", "min_latency_ms", "max_latency_ms", "qps", "qps_ci_low", "qps_ci_high", "alloc_bytes_per_query", "allocs_per_query"},
		out)
}

func WriteMmapVsMemory(path string, rows []MmapVsMemory) error {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			itoa(row.Docs), row.Query, row.OperatorType, f(row.MemoryLatencyMS), f(row.MemoryLatencyCILow), f(row.MemoryLatencyCIHigh),
			f(row.MmapLatencyMS), f(row.MmapLatencyCILow), f(row.MmapLatencyCIHigh),
			f(row.MmapToMemoryRatio), itoa(row.MemoryHits), itoa(row.MmapHits), strconv.FormatBool(row.ResultsEqual),
		})
	}
	return writeCSV(path,
		[]string{"docs", "query", "operator_type", "memory_latency_ms", "memory_latency_ci_low_ms", "memory_latency_ci_high_ms", "mmap_latency_ms", "mmap_latency_ci_low_ms", "mmap_latency_ci_high_ms", "mmap_to_memory_ratio", "memory_hits", "mmap_hits", "results_equal"},
		out)
}

func WriteRankingStats(path string, rows []RankingStats) error {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			row.Query, itoa(row.Hits), itoa(row.TopK), f(row.BooleanOnlyLatencyMS), f(row.BooleanCILow), f(row.BooleanCIHigh),
			f(row.TFIDFLatencyMS), f(row.TFIDFCILow), f(row.TFIDFCIHigh),
			f(row.BM25LatencyMS), f(row.BM25CILow), f(row.BM25CIHigh), f(row.TFIDFOverheadPercent), f(row.BM25OverheadPercent),
			itoa(row.Top1DocIDTFIDF), itoa(row.Top1DocIDBM25), row.Top1TitleTFIDF, row.Top1TitleBM25,
		})
	}
	return writeCSV(path,
		[]string{"query", "hits", "topK", "boolean_only_latency_ms", "boolean_ci_low_ms", "boolean_ci_high_ms", "tfidf_latency_ms", "tfidf_ci_low_ms", "tfidf_ci_high_ms", "bm25_latency_ms", "bm25_ci_low_ms", "bm25_ci_high_ms", "tfidf_overhead_percent", "bm25_overhead_percent", "top1_doc_id_tfidf", "top1_doc_id_bm25", "top1_title_tfidf", "top1_title_bm25"},
		out)
}

func WriteScaleStats(path string, rows []ScaleStats) error {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			row.CorpusName, itoa(row.Docs), itoa(row.Shards), itoa(row.TotalTokens),
			f(row.AvgDocLen), itoa(row.MinDocLen), itoa(row.MaxDocLen), itoa(row.TotalPostings),
			itoa(row.TotalPositions), strconv.FormatInt(row.RawTotalBytes, 10),
			strconv.FormatInt(row.CompressedTotalBytes, 10), strconv.FormatInt(row.SegmentFileSizeBytes, 10),
			f(row.CompressionRatio), f(row.SpaceSavingPercent),
		})
	}
	return writeCSV(path,
		[]string{"corpus_name", "docs", "shards", "total_tokens", "avg_doc_len", "min_doc_len", "max_doc_len", "total_postings", "total_positions", "raw_total_bytes", "compressed_total_bytes", "segment_file_size_bytes", "compression_ratio", "space_saving_percent"},
		out)
}

func itoa(v int) string {
	return strconv.Itoa(v)
}

func f(v float64) string {
	return fmt.Sprintf("%.4f", v)
}
