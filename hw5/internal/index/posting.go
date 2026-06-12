package index

type Posting struct {
	DocID     int
	TF        int
	Positions []int
}

type SkipPointer struct {
	FromIndex   int
	ToIndex     int
	TargetDocID int
	Offset      int64
}

type PostingList struct {
	Term     string
	Postings []Posting
	Skips    []SkipPointer
	DF       int
	TTF      int
}

func NewPostingList(term string, postings []Posting) PostingList {
	pl := PostingList{Term: term, Postings: postings, DF: len(postings)}
	for _, p := range postings {
		pl.TTF += p.TF
	}
	pl.Skips = BuildSkips(postings)
	return pl
}

func BuildSkips(postings []Posting) []SkipPointer {
	df := len(postings)
	if df < 4 {
		return nil
	}
	step := isqrt(df)
	if step < 2 {
		step = 2
	}
	var skips []SkipPointer
	for from := 0; from+step < df; from += step {
		to := from + step
		skips = append(skips, SkipPointer{
			FromIndex:   from,
			ToIndex:     to,
			TargetDocID: postings[to].DocID,
		})
	}
	return skips
}

func isqrt(n int) int {
	x := 0
	for (x+1)*(x+1) <= n {
		x++
	}
	return x
}

type PostingIterator struct {
	list      []Posting
	skips     []SkipPointer
	skipByPos map[int]SkipPointer
	idx       int
}

func NewPostingIterator(pl PostingList) *PostingIterator {
	m := make(map[int]SkipPointer, len(pl.Skips))
	for _, sp := range pl.Skips {
		m[sp.FromIndex] = sp
	}
	return &PostingIterator{list: pl.Postings, skips: pl.Skips, skipByPos: m, idx: -1}
}

func NewSliceIterator(postings []Posting) *PostingIterator {
	return NewPostingIterator(NewPostingList("", postings))
}

func (it *PostingIterator) Next() bool {
	if it.idx+1 >= len(it.list) {
		it.idx = len(it.list)
		return false
	}
	it.idx++
	return true
}

func (it *PostingIterator) DocID() int {
	if it.idx < 0 || it.idx >= len(it.list) {
		return -1
	}
	return it.list[it.idx].DocID
}

func (it *PostingIterator) Posting() Posting {
	if it.idx < 0 || it.idx >= len(it.list) {
		return Posting{}
	}
	return it.list[it.idx]
}

func (it *PostingIterator) Advance(targetDocID int) bool {
	if it.idx < 0 {
		return it.Next() && it.Advance(targetDocID)
	}
	for it.idx < len(it.list) && it.list[it.idx].DocID < targetDocID {
		if sp, ok := it.skipByPos[it.idx]; ok && sp.ToIndex < len(it.list) && sp.TargetDocID <= targetDocID {
			it.idx = sp.ToIndex
			continue
		}
		it.idx++
	}
	return it.idx < len(it.list)
}
