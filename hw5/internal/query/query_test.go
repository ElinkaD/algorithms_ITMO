package query

import (
	"testing"

	"hw5/internal/index"
)

func testIndex() *index.MemoryIndex {
	b := index.NewIndexBuilder()
	_ = b.AddDocument(index.Document{ID: 1, Title: "Data engineering", Text: "data engineer builds data pipeline"})
	_ = b.AddDocument(index.Document{ID: 2, Title: "Analytics", Text: "analytics developer uses python"})
	_ = b.AddDocument(index.Document{ID: 3, Title: "New York", Text: "new york data meetup"})
	_ = b.AddDocument(index.Document{ID: 4, Title: "Pipeline", Text: "data warehouse notes without python"})
	return b.Build()
}

func mustExec(t *testing.T, q string) index.PostingList {
	t.Helper()
	node, err := Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	pl, err := Execute(testIndex(), node)
	if err != nil {
		t.Fatal(err)
	}
	return pl
}

func TestParser(t *testing.T) {
	node, err := Parse(`(data OR analytics) AND engineer AND NOT python`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := node.(AndNode); !ok {
		t.Fatalf("got %T", node)
	}
	phrase, err := Parse(`"new york"`)
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := phrase.(PhraseNode); !ok || len(p.Terms) != 2 {
		t.Fatalf("bad phrase: %#v", phrase)
	}
}

func TestAndOrNotAdjNear(t *testing.T) {
	cases := []struct {
		q    string
		want int
	}{
		{"data AND engineer", 1},
		{"data OR analytics", 4},
		{"data AND NOT python", 2},
		{"new ADJ york", 1},
		{"data NEAR/3 pipeline", 2},
		{`"new york"`, 1},
	}
	for _, tc := range cases {
		got := mustExec(t, tc.q)
		if len(got.Postings) != tc.want {
			t.Fatalf("%q hits=%d want=%d postings=%#v", tc.q, len(got.Postings), tc.want, got.Postings)
		}
	}
}
