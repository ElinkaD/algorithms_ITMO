package scoring

import (
	"container/heap"
	"math"

	"hw5/internal/index"
)

type RankMode string

const (
	RankNone  RankMode = "none"
	RankTFIDF RankMode = "tfidf"
	RankBM25  RankMode = "bm25"
)

type SearchResult struct {
	DocID   int
	Title   string
	Score   float64
	Snippet string
}

func ScoreTFIDF(idx *index.MemoryIndex, docID int, terms []string) float64 {
	var score float64
	for _, term := range unique(terms) {
		tf := termFrequency(idx.Lookup(term), docID)
		if tf == 0 {
			continue
		}
		df := idx.DF(term)
		idf := math.Log(float64(idx.DocCount+1)/float64(df+1)) + 1
		score += (1 + math.Log(float64(tf))) * idf
	}
	return score
}

func TopK(idx *index.MemoryIndex, candidates index.PostingList, terms []string, mode RankMode, k int) []SearchResult {
	if k <= 0 {
		k = 10
	}
	h := &resultHeap{}
	for _, p := range candidates.Postings {
		if mode == RankNone {
			res := SearchResult{DocID: p.DocID, Title: idx.Titles[p.DocID], Score: 0, Snippet: ""}
			if len(*h) < k {
				heap.Push(h, res)
			}
			continue
		}
		score := ScoreTFIDF(idx, p.DocID, terms)
		if mode == RankBM25 {
			score = ScoreBM25(idx, p.DocID, terms)
		}
		if score == 0 {
			continue
		}
		res := SearchResult{DocID: p.DocID, Title: idx.Titles[p.DocID], Score: score, Snippet: ""}
		if h.Len() < k {
			heap.Push(h, res)
			continue
		}
		if (*h)[0].Score < res.Score {
			heap.Pop(h)
			heap.Push(h, res)
		}
	}
	out := make([]SearchResult, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(SearchResult)
	}
	return out
}

func termFrequency(pl index.PostingList, docID int) int {
	lo, hi := 0, len(pl.Postings)
	for lo < hi {
		mid := (lo + hi) / 2
		if pl.Postings[mid].DocID < docID {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(pl.Postings) && pl.Postings[lo].DocID == docID {
		return pl.Postings[lo].TF
	}
	return 0
}

func unique(terms []string) []string {
	seen := make(map[string]struct{}, len(terms))
	out := make([]string, 0, len(terms))
	for _, term := range terms {
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		out = append(out, term)
	}
	return out
}

type resultHeap []SearchResult

func (h resultHeap) Len() int           { return len(h) }
func (h resultHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h resultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *resultHeap) Push(x any)        { *h = append(*h, x.(SearchResult)) }
func (h *resultHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
