package benchdata

import (
	"fmt"
	"math/rand"
	"strings"

	"hw5/internal/index"
)

func GenerateDocuments(n int) []index.Document {
	r := rand.New(rand.NewSource(42 + int64(n)))
	high := []string{"data", "engineer", "analytics", "pipeline", "python", "storage", "query", "index", "search", "memory", "system", "distributed", "machine", "learning", "new", "york", "alpha", "beta"}
	phrases := [][]string{
		{"data", "pipeline"},
		{"new", "york"},
		{"machine", "learning"},
		{"distributed", "systems"},
		{"alpha", "beta"},
	}
	docs := make([]index.Document, 0, n)
	for id := 1; id <= n; id++ {
		length := 100 + r.Intn(201)
		words := make([]string, 0, length+8)
		if id%7 == 0 {
			words = append(words, phrases[(id/7)%len(phrases)]...)
		}
		if id%11 == 0 {
			words = append(words, "data", "warehouse", "pipeline")
		}
		for len(words) < length {
			x := r.Intn(100)
			switch {
			case x < 55:
				words = append(words, high[r.Intn(len(high))])
			case x < 92:
				words = append(words, fmt.Sprintf("term%d", r.Intn(10000)))
			default:
				words = append(words, fmt.Sprintf("rare%d", r.Intn(20000)))
			}
		}
		docs = append(docs, index.Document{
			ID:    id,
			Title: fmt.Sprintf("Synthetic document %d", id),
			Text:  strings.Join(words, " "),
		})
	}
	return docs
}

func BuildIndex(docs []index.Document) *index.MemoryIndex {
	builder := index.NewIndexBuilder()
	for _, doc := range docs {
		_ = builder.AddDocument(doc)
	}
	return builder.Build()
}

func DemoDocuments() []index.Document {
	return []index.Document{
		{ID: 1, Title: "Data engineering basics", Text: "data engineer builds reliable data pipeline systems"},
		{ID: 2, Title: "Python scripts", Text: "python developer writes automation scripts"},
		{ID: 3, Title: "Distributed data systems", Text: "distributed data systems use indexes and storage"},
		{ID: 4, Title: "Data pipeline architecture", Text: "modern data pipeline architecture connects storage analytics and monitoring"},
		{ID: 5, Title: "New York data meetup", Text: "new york hosts data engineering meetup"},
		{ID: 6, Title: "Data warehouse notes", Text: "data warehouse notes discuss analytics without scripting"},
		{ID: 7, Title: "Machine learning intro", Text: "machine learning models consume data"},
		{ID: 8, Title: "Search index internals", Text: "inverted index keeps postings positions and term frequency"},
		{ID: 9, Title: "Analytics developer", Text: "analytics developer studies dashboards"},
		{ID: 10, Title: "Pipeline monitoring", Text: "monitoring data pipeline latency and throughput"},
		{ID: 11, Title: "Text retrieval", Text: "information retrieval uses tf idf and bm25 ranking"},
		{ID: 12, Title: "Storage formats", Text: "binary storage format uses mmap segments"},
		{ID: 13, Title: "Compression", Text: "delta encoding pfordelta and bitpacking compress postings"},
		{ID: 14, Title: "Query parser", Text: "query parser supports and or not near adjacent"},
		{ID: 15, Title: "Systems benchmark", Text: "benchmark profiles cpu memory and allocations"},
		{ID: 16, Title: "Go implementation", Text: "go implementation controls memory allocations"},
		{ID: 17, Title: "Developer tools", Text: "developer tools include makefile scripts and reports"},
		{ID: 18, Title: "Near query", Text: "data moves through robust batch pipeline stages"},
		{ID: 19, Title: "Index builder", Text: "builder collects terms positions and document lengths"},
		{ID: 20, Title: "Ranking notes", Text: "bm25 uses document length normalization"},
	}
}
