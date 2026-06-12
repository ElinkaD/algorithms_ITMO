package query

type Node interface {
	QueryTerms() []string
}

type TermNode struct {
	Term string
}

type PhraseNode struct {
	Terms []string
}

type AndNode struct {
	Children []Node
}

type OrNode struct {
	Children []Node
}

type NotNode struct {
	Child Node
}

type AdjNode struct {
	Left  Node
	Right Node
}

type NearNode struct {
	Left  Node
	Right Node
	K     int
}

func (n TermNode) QueryTerms() []string   { return []string{n.Term} }
func (n PhraseNode) QueryTerms() []string { return append([]string(nil), n.Terms...) }
func (n AndNode) QueryTerms() []string    { return collectTerms(n.Children) }
func (n OrNode) QueryTerms() []string     { return collectTerms(n.Children) }
func (n NotNode) QueryTerms() []string    { return n.Child.QueryTerms() }
func (n AdjNode) QueryTerms() []string    { return append(n.Left.QueryTerms(), n.Right.QueryTerms()...) }
func (n NearNode) QueryTerms() []string   { return append(n.Left.QueryTerms(), n.Right.QueryTerms()...) }

func collectTerms(nodes []Node) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, node := range nodes {
		for _, term := range node.QueryTerms() {
			if _, ok := seen[term]; ok {
				continue
			}
			seen[term] = struct{}{}
			out = append(out, term)
		}
	}
	return out
}
