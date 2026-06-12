package scoring

import (
	"testing"

	"hw5/internal/index"
)

func idx() *index.MemoryIndex {
	b := index.NewIndexBuilder()
	_ = b.AddDocument(index.Document{ID: 1, Title: "A", Text: "data data engineer"})
	_ = b.AddDocument(index.Document{ID: 2, Title: "B", Text: "python engineer"})
	return b.Build()
}

func TestTFIDFAndBM25(t *testing.T) {
	idx := idx()
	terms := []string{"data", "engineer"}
	if s := ScoreTFIDF(idx, 1, terms); s <= 0 {
		t.Fatalf("tfidf score=%f", s)
	}
	if s := ScoreBM25(idx, 1, terms); s <= 0 {
		t.Fatalf("bm25 score=%f", s)
	}
	results := TopK(idx, idx.Lookup("engineer"), terms, RankBM25, 1)
	if len(results) != 1 {
		t.Fatalf("topk len=%d", len(results))
	}
}
