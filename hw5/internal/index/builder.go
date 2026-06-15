package index

import (
	"fmt"
	"sort"

	"hw5/internal/analyzer"
)

type IndexBuilder struct {
	postings map[string]map[int][]int
	docLens  map[int]int
	titles   map[int]string
	analyzer analyzer.Analyzer
}

func NewIndexBuilder() *IndexBuilder {
	return &IndexBuilder{
		postings: make(map[string]map[int][]int),
		docLens:  make(map[int]int),
		titles:   make(map[int]string),
	}
}

func (b *IndexBuilder) AddDocument(doc Document) error {
	if doc.ID < 0 {
		return fmt.Errorf("negative doc id: %d", doc.ID)
	}
	tokens := b.analyzer.Analyze(doc.Title + " " + doc.Text)
	b.docLens[doc.ID] = len(tokens)
	b.titles[doc.ID] = doc.Title
	for _, tok := range tokens {
		if b.postings[tok.Term] == nil {
			b.postings[tok.Term] = make(map[int][]int)
		}
		b.postings[tok.Term][doc.ID] = append(b.postings[tok.Term][doc.ID], tok.Position)
	}
	return nil
}

func (b *IndexBuilder) Build() *MemoryIndex {
	idx := &MemoryIndex{
		Postings:  make(map[string]PostingList, len(b.postings)),
		DocLens:   make(map[int]int, len(b.docLens)),
		Titles:    make(map[int]string, len(b.titles)),
		DocCount:  len(b.docLens),
		TermStats: make(map[string]TermStat, len(b.postings)),
	}
	totalLen := 0
	for docID, l := range b.docLens {
		idx.DocLens[docID] = l
		totalLen += l
	}
	for docID, title := range b.titles {
		idx.Titles[docID] = title
	}
	if idx.DocCount > 0 {
		idx.AvgDocLen = float64(totalLen) / float64(idx.DocCount)
	}
	for term, byDoc := range b.postings {
		docIDs := make([]int, 0, len(byDoc))
		for docID := range byDoc {
			docIDs = append(docIDs, docID)
		}
		sort.Ints(docIDs)
		postings := make([]Posting, 0, len(docIDs))
		ttf := 0
		for _, docID := range docIDs {
			positions := append([]int(nil), byDoc[docID]...)
			sort.Ints(positions)
			ttf += len(positions)
			postings = append(postings, Posting{DocID: docID, TF: len(positions), Positions: positions})
		}
		pl := NewPostingList(term, postings)
		idx.Postings[term] = pl
		idx.TermStats[term] = TermStat{DF: len(postings), TTF: ttf}
	}
	return idx
}
