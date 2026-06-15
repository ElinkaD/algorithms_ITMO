package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"hw5/internal/benchdata"
	"hw5/internal/codec"
	"hw5/internal/index"
	"hw5/internal/query"
	"hw5/internal/scoring"
	"hw5/internal/storage"
)

func benchmarkBuildIndex(b *testing.B, n int) {
	docs := benchdata.GenerateDocuments(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchdata.BuildIndex(docs)
	}
}

func BenchmarkBuildIndex_1k(b *testing.B)  { benchmarkBuildIndex(b, 1000) }
func BenchmarkBuildIndex_5k(b *testing.B)  { benchmarkBuildIndex(b, 5000) }
func BenchmarkBuildIndex_10k(b *testing.B) { benchmarkBuildIndex(b, 10000) }

func benchmarkQuery(b *testing.B, q string) {
	idx := benchdata.BuildIndex(benchdata.GenerateDocuments(5000))
	node, err := query.Parse(q)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := query.Execute(idx, node); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAndQuery(b *testing.B)  { benchmarkQuery(b, "data AND pipeline") }
func BenchmarkOrQuery(b *testing.B)   { benchmarkQuery(b, "data OR pipeline") }
func BenchmarkNotQuery(b *testing.B)  { benchmarkQuery(b, "data AND NOT python") }
func BenchmarkAdjQuery(b *testing.B)  { benchmarkQuery(b, "data ADJ pipeline") }
func BenchmarkNearQuery(b *testing.B) { benchmarkQuery(b, "data NEAR/5 pipeline") }

func BenchmarkBM25TopK(b *testing.B) {
	idx := benchdata.BuildIndex(benchdata.GenerateDocuments(5000))
	node, _ := query.Parse("data AND pipeline")
	pl, _ := query.Execute(idx, node)
	terms := node.QueryTerms()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scoring.TopK(idx, pl, terms, scoring.RankBM25, 10)
	}
}

func BenchmarkCompressionEncode(b *testing.B) {
	values := make([]uint32, 10000)
	for i := range values {
		values[i] = uint32(i%31 + 1)
		if i%97 == 0 {
			values[i] = uint32(100000 + i)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = codec.Encode(values)
	}
}

func BenchmarkCompressionDecode(b *testing.B) {
	values := make([]uint32, 10000)
	for i := range values {
		values[i] = uint32(i%31 + 1)
		if i%97 == 0 {
			values[i] = uint32(100000 + i)
		}
	}
	encoded := codec.Encode(values)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = codec.Decode(encoded)
	}
}

func BenchmarkMmapTermLookup(b *testing.B) {
	idx := benchdata.BuildIndex(benchdata.GenerateDocuments(5000))
	path := filepath.Join(b.TempDir(), "bench.seg")
	if err := storage.Write(idx, path); err != nil {
		b.Fatal(err)
	}
	r, err := storage.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.LookupTerm("data"); err != nil {
			b.Fatal(err)
		}
	}
}

func wikiFixture(b *testing.B) (*index.MemoryIndex, []benchdata.QuerySpec, string) {
	b.Helper()
	if os.Getenv("RUN_WIKI_BENCH") != "1" {
		b.Skip("set RUN_WIKI_BENCH=1 to run Wikipedia benchmarks")
	}
	wikiPath := os.Getenv("WIKI")
	if wikiPath == "" {
		wikiPath = "./data/wiki_sample.jsonl"
	}
	docsLimit := 5000
	if raw := os.Getenv("DOCS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			docsLimit = n
		}
	}
	docs, err := benchdata.ReadWikiJSONL(wikiPath, docsLimit)
	if err != nil {
		b.Fatalf("read wiki: %v", err)
	}
	idx := benchdata.BuildIndex(docs)
	specs := benchdata.GenerateQuerySuite(idx, docs, 5)
	return idx, specs, wikiPath
}

func firstSpec(b *testing.B, specs []benchdata.QuerySpec, typ string) benchdata.QuerySpec {
	b.Helper()
	for _, spec := range specs {
		if spec.OperatorType == typ {
			return spec
		}
	}
	b.Skipf("no wiki query of type %s", typ)
	return benchdata.QuerySpec{}
}

func benchmarkWikiQuery(b *testing.B, typ string) {
	idx, specs, _ := wikiFixture(b)
	spec := firstSpec(b, specs, typ)
	node, err := query.Parse(spec.Query)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := query.Execute(idx, node); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWikiBuild_5k(b *testing.B) {
	if os.Getenv("RUN_WIKI_BENCH") != "1" {
		b.Skip("set RUN_WIKI_BENCH=1 to run Wikipedia benchmarks")
	}
	wikiPath := os.Getenv("WIKI")
	if wikiPath == "" {
		wikiPath = "./data/wiki_sample.jsonl"
	}
	docs, err := benchdata.ReadWikiJSONL(wikiPath, 5000)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchdata.BuildIndex(docs)
	}
}

func BenchmarkWikiAndQuery(b *testing.B)     { benchmarkWikiQuery(b, "AND") }
func BenchmarkWikiOrQuery(b *testing.B)      { benchmarkWikiQuery(b, "OR") }
func BenchmarkWikiNotQuery(b *testing.B)     { benchmarkWikiQuery(b, "NOT") }
func BenchmarkWikiAdjQuery(b *testing.B)     { benchmarkWikiQuery(b, "ADJ") }
func BenchmarkWikiNearQuery(b *testing.B)    { benchmarkWikiQuery(b, "NEAR/3") }
func BenchmarkWikiPhraseQuery(b *testing.B)  { benchmarkWikiQuery(b, "PHRASE") }
func BenchmarkWikiComplexQuery(b *testing.B) { benchmarkWikiQuery(b, "COMPLEX") }

func BenchmarkWikiBM25TopK(b *testing.B) {
	idx, specs, _ := wikiFixture(b)
	spec := firstSpec(b, specs, "AND")
	node, _ := query.Parse(spec.Query)
	pl, _ := query.Execute(idx, node)
	terms := node.QueryTerms()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scoring.TopK(idx, pl, terms, scoring.RankBM25, 10)
	}
}

func BenchmarkWikiTFIDFTopK(b *testing.B) {
	idx, specs, _ := wikiFixture(b)
	spec := firstSpec(b, specs, "AND")
	node, _ := query.Parse(spec.Query)
	pl, _ := query.Execute(idx, node)
	terms := node.QueryTerms()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scoring.TopK(idx, pl, terms, scoring.RankTFIDF, 10)
	}
}

func BenchmarkWikiMmapLookup(b *testing.B) {
	idx, specs, _ := wikiFixture(b)
	spec := firstSpec(b, specs, "TERM")
	path := filepath.Join(b.TempDir(), "wiki.seg")
	if err := storage.Write(idx, path); err != nil {
		b.Fatal(err)
	}
	r, err := storage.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	term := spec.Terms[0]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.LookupTerm(term); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWikiMemoryVsMmapAnd(b *testing.B) {
	idx, specs, _ := wikiFixture(b)
	spec := firstSpec(b, specs, "AND")
	path := filepath.Join(b.TempDir(), "wiki.seg")
	if err := storage.Write(idx, path); err != nil {
		b.Fatal(err)
	}
	node, _ := query.Parse(spec.Query)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := storage.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		mmapIdx, err := r.Materialize(node.QueryTerms())
		_ = r.Close()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := query.Execute(mmapIdx, node); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWikiCompressionStats(b *testing.B) {
	idx, _, _ := wikiFixture(b)
	path := filepath.Join(b.TempDir(), "wiki.seg")
	if err := storage.Write(idx, path); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := storage.ComputeSizeStats("wiki", idx, path); err != nil {
			b.Fatal(err)
		}
	}
}
