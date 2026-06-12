package query

import (
	"container/heap"
	"fmt"
	"sort"

	"hw5/internal/index"
)

func Execute(idx *index.MemoryIndex, node Node) (index.PostingList, error) {
	switch n := node.(type) {
	case TermNode:
		return idx.Lookup(n.Term), nil
	case PhraseNode:
		return executePhrase(idx, n)
	case AndNode:
		return executeAnd(idx, n.Children)
	case OrNode:
		return executeOr(idx, n.Children)
	case NotNode:
		return index.PostingList{}, fmt.Errorf("NOT must be used together with a positive expression")
	case AdjNode:
		return executeNear(idx, n.Left, n.Right, 1, true)
	case NearNode:
		return executeNear(idx, n.Left, n.Right, n.K, false)
	default:
		return index.PostingList{}, fmt.Errorf("unknown query node %T", node)
	}
}

func executePhrase(idx *index.MemoryIndex, n PhraseNode) (index.PostingList, error) {
	if len(n.Terms) == 1 {
		return idx.Lookup(n.Terms[0]), nil
	}
	var node Node = TermNode{Term: n.Terms[0]}
	for _, term := range n.Terms[1:] {
		node = AdjNode{Left: node, Right: TermNode{Term: term}}
	}
	return Execute(idx, node)
}

func executeAnd(idx *index.MemoryIndex, children []Node) (index.PostingList, error) {
	var positives []Node
	var negatives []Node
	for _, child := range children {
		if n, ok := child.(NotNode); ok {
			negatives = append(negatives, n.Child)
		} else {
			positives = append(positives, child)
		}
	}
	if len(positives) == 0 {
		return index.PostingList{}, fmt.Errorf("NOT cannot be executed without a positive clause")
	}
	sort.SliceStable(positives, func(i, j int) bool {
		return estimateCost(idx, positives[i]) < estimateCost(idx, positives[j])
	})
	base, err := Execute(idx, positives[0])
	if err != nil {
		return index.PostingList{}, err
	}
	for _, child := range positives[1:] {
		next, err := Execute(idx, child)
		if err != nil {
			return index.PostingList{}, err
		}
		base = intersect(base, next)
	}
	for _, child := range negatives {
		neg, err := Execute(idx, child)
		if err != nil {
			return index.PostingList{}, err
		}
		base = subtract(base, neg)
	}
	base.Term = ""
	return index.NewPostingList("", base.Postings), nil
}

func executeOr(idx *index.MemoryIndex, children []Node) (index.PostingList, error) {
	h := &orHeap{}
	for i, child := range children {
		pl, err := Execute(idx, child)
		if err != nil {
			return index.PostingList{}, err
		}
		it := index.NewPostingIterator(pl)
		if it.Next() {
			heap.Push(h, orItem{docID: it.DocID(), iter: it, source: i})
		}
	}
	var out []index.Posting
	for h.Len() > 0 {
		item := heap.Pop(h).(orItem)
		docID := item.docID
		merged := item.iter.Posting()
		for item.iter.Next() {
			heap.Push(h, orItem{docID: item.iter.DocID(), iter: item.iter, source: item.source})
			break
		}
		for h.Len() > 0 && (*h)[0].docID == docID {
			same := heap.Pop(h).(orItem)
			p := same.iter.Posting()
			merged.TF += p.TF
			merged.Positions = append(merged.Positions, p.Positions...)
			if same.iter.Next() {
				heap.Push(h, orItem{docID: same.iter.DocID(), iter: same.iter, source: same.source})
			}
		}
		sort.Ints(merged.Positions)
		out = append(out, merged)
	}
	return index.NewPostingList("", out), nil
}

func executeNear(idx *index.MemoryIndex, left Node, right Node, k int, adjacent bool) (index.PostingList, error) {
	lpl, err := Execute(idx, left)
	if err != nil {
		return index.PostingList{}, err
	}
	rpl, err := Execute(idx, right)
	if err != nil {
		return index.PostingList{}, err
	}
	li, ri := index.NewPostingIterator(lpl), index.NewPostingIterator(rpl)
	if !li.Next() || !ri.Next() {
		return index.PostingList{}, nil
	}
	var out []index.Posting
	for {
		ld, rd := li.DocID(), ri.DocID()
		if ld == rd {
			if positionsNear(li.Posting().Positions, ri.Posting().Positions, k, adjacent) {
				p := li.Posting()
				p.TF += ri.Posting().TF
				p.Positions = append(append([]int(nil), p.Positions...), ri.Posting().Positions...)
				sort.Ints(p.Positions)
				out = append(out, p)
			}
			if !li.Next() || !ri.Next() {
				break
			}
			continue
		}
		if ld < rd {
			if !li.Advance(rd) {
				break
			}
		} else {
			if !ri.Advance(ld) {
				break
			}
		}
	}
	return index.NewPostingList("", out), nil
}

func intersect(a, b index.PostingList) index.PostingList {
	ia, ib := index.NewPostingIterator(a), index.NewPostingIterator(b)
	if !ia.Next() || !ib.Next() {
		return index.PostingList{}
	}
	var out []index.Posting
	for {
		da, db := ia.DocID(), ib.DocID()
		if da == db {
			p := ia.Posting()
			q := ib.Posting()
			p.TF += q.TF
			p.Positions = append(append([]int(nil), p.Positions...), q.Positions...)
			sort.Ints(p.Positions)
			out = append(out, p)
			if !ia.Next() || !ib.Next() {
				break
			}
			continue
		}
		if da < db {
			if !ia.Advance(db) {
				break
			}
		} else if !ib.Advance(da) {
			break
		}
	}
	return index.NewPostingList("", out)
}

func subtract(pos, neg index.PostingList) index.PostingList {
	ip, in := index.NewPostingIterator(pos), index.NewPostingIterator(neg)
	if !ip.Next() {
		return index.PostingList{}
	}
	hasNeg := in.Next()
	var out []index.Posting
	for {
		docID := ip.DocID()
		for hasNeg && in.DocID() < docID {
			hasNeg = in.Advance(docID)
		}
		if !hasNeg || in.DocID() != docID {
			out = append(out, ip.Posting())
		}
		if !ip.Next() {
			break
		}
	}
	return index.NewPostingList("", out)
}

func positionsNear(a, b []int, k int, adjacent bool) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		diff := b[j] - a[i]
		if adjacent {
			if diff == 1 {
				return true
			}
		} else if diff < 0 {
			if -diff <= k {
				return true
			}
		} else if diff <= k {
			return true
		}
		if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return false
}

func estimateCost(idx *index.MemoryIndex, node Node) int {
	terms := node.QueryTerms()
	if len(terms) == 0 {
		return idx.DocCount
	}
	cost := idx.DocCount + 1
	for _, term := range terms {
		if df := idx.DF(term); df < cost {
			cost = df
		}
	}
	return cost
}

type orItem struct {
	docID  int
	iter   *index.PostingIterator
	source int
}

type orHeap []orItem

func (h orHeap) Len() int           { return len(h) }
func (h orHeap) Less(i, j int) bool { return h[i].docID < h[j].docID }
func (h orHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *orHeap) Push(x any)        { *h = append(*h, x.(orItem)) }
func (h *orHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
