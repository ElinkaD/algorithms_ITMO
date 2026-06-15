package index

import "sort"

type TermStat struct {
	DF  int
	TTF int
}

type MemoryIndex struct {
	Postings  map[string]PostingList
	DocLens   map[int]int
	Titles    map[int]string
	DocCount  int
	AvgDocLen float64
	TermStats map[string]TermStat
}

func (m *MemoryIndex) Lookup(term string) PostingList {
	if m == nil {
		return PostingList{Term: term}
	}
	if pl, ok := m.Postings[term]; ok {
		return pl
	}
	return PostingList{Term: term}
}

func (m *MemoryIndex) DF(term string) int {
	if stat, ok := m.TermStats[term]; ok {
		return stat.DF
	}
	return 0
}

func (m *MemoryIndex) Terms() []string {
	terms := make([]string, 0, len(m.Postings))
	for term := range m.Postings {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}

func (m *MemoryIndex) RawPostingsSize() int64 {
	var size int64
	for _, pl := range m.Postings {
		for _, p := range pl.Postings {
			size += 8
			size += int64(len(p.Positions) * 4)
		}
		size += int64(len(pl.Skips) * 24)
	}
	return size
}
