package scoring

import (
	"math"

	"hw5/internal/index"
)

const (
	BM25K1 = 1.2
	BM25B  = 0.75
)

func ScoreBM25(idx *index.MemoryIndex, docID int, terms []string) float64 {
	var score float64
	docLen := float64(idx.DocLens[docID])
	if docLen == 0 {
		docLen = idx.AvgDocLen
	}
	if idx.AvgDocLen == 0 {
		return 0
	}
	for _, term := range unique(terms) {
		tf := float64(termFrequency(idx.Lookup(term), docID))
		if tf == 0 {
			continue
		}
		df := float64(idx.DF(term))
		n := float64(idx.DocCount)
		idf := math.Log(1 + (n-df+0.5)/(df+0.5))
		norm := tf + BM25K1*(1-BM25B+BM25B*docLen/idx.AvgDocLen)
		score += idf * (tf * (BM25K1 + 1) / norm)
	}
	return score
}
