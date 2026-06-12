package analyzer

import "testing"

func TestTokenize(t *testing.T) {
	got := Tokenize("Data, инженерия! New-York 2026")
	want := []Token{
		{Term: "data", Position: 0},
		{Term: "инженерия", Position: 1},
		{Term: "new", Position: 2},
		{Term: "york", Position: 3},
		{Term: "2026", Position: 4},
	}
	if len(got) != len(want) {
		t.Fatalf("len=%d want=%d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token %d=%#v want %#v", i, got[i], want[i])
		}
	}
}
