package storage

import (
	"path/filepath"
	"testing"

	"hw5/internal/index"
)

func TestSegmentWriteReadAndMmapLookup(t *testing.T) {
	b := index.NewIndexBuilder()
	_ = b.AddDocument(index.Document{ID: 1, Title: "One", Text: "data data pipeline"})
	_ = b.AddDocument(index.Document{ID: 2, Title: "Two", Text: "python pipeline"})
	idx := b.Build()
	path := filepath.Join(t.TempDir(), "idx.seg")
	if err := Write(idx, path); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Stats().DocCount != 2 || r.Stats().TermCount == 0 {
		t.Fatalf("bad stats: %#v", r.Stats())
	}
	pl, err := r.LookupTerm("data")
	if err != nil {
		t.Fatal(err)
	}
	if pl.DF != 1 || len(pl.Postings) != 1 || pl.Postings[0].TF != 2 {
		t.Fatalf("bad posting list: %#v", pl)
	}
	mem, err := r.Materialize([]string{"pipeline"})
	if err != nil {
		t.Fatal(err)
	}
	if mem.DF("pipeline") != 2 {
		t.Fatalf("df=%d", mem.DF("pipeline"))
	}
}
