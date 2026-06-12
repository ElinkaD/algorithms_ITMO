package index

import "testing"

func TestIndexBuilder(t *testing.T) {
	b := NewIndexBuilder()
	_ = b.AddDocument(Document{ID: 2, Title: "B", Text: "data data pipeline"})
	_ = b.AddDocument(Document{ID: 1, Title: "A", Text: "data engineer"})
	idx := b.Build()
	if idx.DocCount != 2 {
		t.Fatalf("DocCount=%d", idx.DocCount)
	}
	pl := idx.Lookup("data")
	if pl.DF != 2 || len(pl.Postings) != 2 {
		t.Fatalf("bad df/postings: %#v", pl)
	}
	if pl.Postings[0].DocID != 1 || pl.Postings[1].DocID != 2 {
		t.Fatalf("postings are not sorted: %#v", pl.Postings)
	}
	if idx.TermStats["data"].TTF != 3 {
		t.Fatalf("ttf=%d", idx.TermStats["data"].TTF)
	}
}
