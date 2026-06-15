package benchdata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"hw5/internal/analyzer"
	"hw5/internal/index"
	"hw5/internal/query"
)

type QuerySpec struct {
	Query            string   `json:"query"`
	OperatorType     string   `json:"operator_type"`
	ExpectedNonEmpty bool     `json:"expected_non_empty"`
	MatchCount       int      `json:"match_count"`
	Terms            []string `json:"terms"`
	Notes            string   `json:"notes,omitempty"`
}

func GenerateQuerySuite(idx *index.MemoryIndex, docs []index.Document, perType int) []QuerySpec {
	if perType <= 0 {
		perType = 10
	}
	topLimit := perType * 3
	if topLimit < 80 {
		topLimit = 80
	}
	topTerms := topTermsByDF(idx, topLimit)
	var suite []QuerySpec
	add := func(q string, typ string, notes string) {
		if countType(suite, typ) >= perType {
			return
		}
		node, err := query.Parse(q)
		if err != nil {
			return
		}
		pl, err := query.Execute(idx, node)
		if err != nil || len(pl.Postings) == 0 {
			return
		}
		suite = append(suite, QuerySpec{
			Query:            q,
			OperatorType:     typ,
			ExpectedNonEmpty: true,
			MatchCount:       len(pl.Postings),
			Terms:            node.QueryTerms(),
			Notes:            notes,
		})
	}
	for _, seed := range thematicQuerySeeds() {
		add(seed.Query, seed.OperatorType, seed.Notes)
	}
	for _, term := range topTerms {
		add(term, "TERM", "high-df term from corpus")
		add(term, "BM25_TOPK", "same boolean candidates, BM25 ranking")
		add(term, "TFIDF_TOPK", "same boolean candidates, TF-IDF ranking")
		if hasEnoughTypes(suite, perType, "TERM", "BM25_TOPK", "TFIDF_TOPK") {
			break
		}
	}
	for i := 0; i < len(topTerms); i++ {
		for j := i + 1; j < len(topTerms); j++ {
			a, b := topTerms[i], topTerms[j]
			add(fmt.Sprintf("%s AND %s", a, b), "AND", "top term pair")
			add(fmt.Sprintf("%s OR %s", a, b), "OR", "top term pair")
			add(fmt.Sprintf("%s AND NOT %s", a, b), "NOT", "positive term minus another term")
			if countType(suite, "COMPLEX") < perType && j+1 < len(topTerms) {
				c := topTerms[j+1]
				add(fmt.Sprintf("(%s OR %s) AND %s", a, b, c), "COMPLEX", "mixed OR/AND")
				add(fmt.Sprintf("(%s OR %s) AND NOT %s", a, b, c), "COMPLEX", "mixed OR/NOT")
			}
			if hasEnoughTypes(suite, perType, "AND", "OR", "NOT", "COMPLEX") {
				break
			}
		}
		if hasEnoughTypes(suite, perType, "AND", "OR", "NOT", "COMPLEX") {
			break
		}
	}
	for _, pair := range adjacentPairs(docs, perType*4) {
		add(pair[0]+" ADJ "+pair[1], "ADJ", "adjacent pair observed in corpus")
		add(pair[0]+" NEAR/3 "+pair[1], "NEAR/3", "near pair from observed adjacent pair")
		add(`"`+pair[0]+" "+pair[1]+`"`, "PHRASE", "phrase sugar for ADJ chain")
		if countType(suite, "COMPLEX") < perType && len(topTerms) > 0 {
			add(pair[0]+" NEAR/3 "+pair[1]+" AND NOT "+topTerms[0], "COMPLEX", "near plus negative clause")
		}
		if hasEnoughTypes(suite, perType, "ADJ", "NEAR/3", "PHRASE") {
			break
		}
	}
	sort.SliceStable(suite, func(i, j int) bool {
		if suite[i].OperatorType == suite[j].OperatorType {
			return suite[i].Query < suite[j].Query
		}
		return suite[i].OperatorType < suite[j].OperatorType
	})
	return suite
}

func thematicQuerySeeds() []QuerySpec {
	return []QuerySpec{
		{Query: "women", OperatorType: "TERM", Notes: "thematic seed: women history and rights"},
		{Query: "feminism", OperatorType: "TERM", Notes: "thematic seed: feminism"},
		{Query: "feminist", OperatorType: "TERM", Notes: "thematic seed: feminist movement"},
		{Query: "suffrage", OperatorType: "TERM", Notes: "thematic seed: voting rights"},
		{Query: "scientist", OperatorType: "TERM", Notes: "thematic seed: science"},
		{Query: "scientists", OperatorType: "TERM", Notes: "thematic seed: science"},
		{Query: "education", OperatorType: "TERM", Notes: "thematic seed: education"},
		{Query: "equality", OperatorType: "TERM", Notes: "thematic seed: equality"},
		{Query: "rights", OperatorType: "TERM", Notes: "thematic seed: rights"},
		{Query: "vote", OperatorType: "TERM", Notes: "thematic seed: voting"},

		{Query: "women AND science", OperatorType: "AND", Notes: "thematic seed: women in science"},
		{Query: "women AND rights", OperatorType: "AND", Notes: "thematic seed: women's rights"},
		{Query: "women AND education", OperatorType: "AND", Notes: "thematic seed: education"},
		{Query: "women AND suffrage", OperatorType: "AND", Notes: "thematic seed: suffrage"},
		{Query: "feminism AND women", OperatorType: "AND", Notes: "thematic seed: feminism"},
		{Query: "feminist AND movement", OperatorType: "AND", Notes: "thematic seed: feminist movement"},
		{Query: "female AND scientists", OperatorType: "AND", Notes: "thematic seed: women scientists"},
		{Query: "equality AND rights", OperatorType: "AND", Notes: "thematic seed: equality rights"},
		{Query: "women AND vote", OperatorType: "AND", Notes: "thematic seed: voting rights"},
		{Query: "women AND university", OperatorType: "AND", Notes: "thematic seed: women and education"},

		{Query: "women OR feminism", OperatorType: "OR", Notes: "thematic seed: broad feminism query"},
		{Query: "women OR female", OperatorType: "OR", Notes: "thematic seed: women/female"},
		{Query: "scientist OR scientists", OperatorType: "OR", Notes: "thematic seed: science terms"},
		{Query: "suffrage OR vote", OperatorType: "OR", Notes: "thematic seed: voting"},
		{Query: "rights OR equality", OperatorType: "OR", Notes: "thematic seed: rights/equality"},

		{Query: "women AND NOT men", OperatorType: "NOT", Notes: "thematic seed: positive women query"},
		{Query: "feminism AND NOT anarchism", OperatorType: "NOT", Notes: "thematic seed: feminism without adjacent political topic"},
		{Query: "science AND NOT computer", OperatorType: "NOT", Notes: "thematic seed: science excluding computer"},
		{Query: "rights AND NOT copyright", OperatorType: "NOT", Notes: "thematic seed: rights excluding copyright"},

		{Query: "women NEAR/3 rights", OperatorType: "NEAR/3", Notes: "thematic seed: women's rights"},
		{Query: "women NEAR/3 science", OperatorType: "NEAR/3", Notes: "thematic seed: women in science"},
		{Query: "women NEAR/3 education", OperatorType: "NEAR/3", Notes: "thematic seed: education"},
		{Query: "female NEAR/3 scientists", OperatorType: "NEAR/3", Notes: "thematic seed: women scientists"},
		{Query: "voting NEAR/3 rights", OperatorType: "NEAR/3", Notes: "thematic seed: voting rights"},

		{Query: "women ADJ rights", OperatorType: "ADJ", Notes: "thematic seed: women's rights phrase"},
		{Query: "voting ADJ rights", OperatorType: "ADJ", Notes: "thematic seed: voting rights"},
		{Query: "female ADJ scientists", OperatorType: "ADJ", Notes: "thematic seed: women scientists"},
		{Query: "feminist ADJ movement", OperatorType: "ADJ", Notes: "thematic seed: feminist movement"},

		{Query: `"women rights"`, OperatorType: "PHRASE", Notes: "thematic seed: phrase"},
		{Query: `"voting rights"`, OperatorType: "PHRASE", Notes: "thematic seed: phrase"},
		{Query: `"female scientists"`, OperatorType: "PHRASE", Notes: "thematic seed: phrase"},
		{Query: `"feminist movement"`, OperatorType: "PHRASE", Notes: "thematic seed: phrase"},
		{Query: `"marie curie"`, OperatorType: "PHRASE", Notes: "thematic seed: scientist name"},
		{Query: `"ada lovelace"`, OperatorType: "PHRASE", Notes: "thematic seed: scientist name"},

		{Query: "(women OR feminism) AND rights", OperatorType: "COMPLEX", Notes: "thematic seed: feminism and rights"},
		{Query: "(women OR female) AND scientists", OperatorType: "COMPLEX", Notes: "thematic seed: women scientists"},
		{Query: "(feminism OR feminist) AND movement", OperatorType: "COMPLEX", Notes: "thematic seed: feminist movement"},
		{Query: "women NEAR/3 rights AND NOT men", OperatorType: "COMPLEX", Notes: "thematic seed: women rights with negative clause"},

		{Query: "women AND science", OperatorType: "BM25_TOPK", Notes: "thematic seed: BM25 women in science"},
		{Query: "women AND rights", OperatorType: "BM25_TOPK", Notes: "thematic seed: BM25 women's rights"},
		{Query: "feminism AND women", OperatorType: "BM25_TOPK", Notes: "thematic seed: BM25 feminism"},
		{Query: "women AND science", OperatorType: "TFIDF_TOPK", Notes: "thematic seed: TF-IDF women in science"},
		{Query: "women AND rights", OperatorType: "TFIDF_TOPK", Notes: "thematic seed: TF-IDF women's rights"},
		{Query: "feminism AND women", OperatorType: "TFIDF_TOPK", Notes: "thematic seed: TF-IDF feminism"},
	}
}

func WriteQuerySuite(pathJSON, pathTXT string, specs []QuerySpec) error {
	if err := os.MkdirAll(filepath.Dir(pathJSON), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(specs, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(pathJSON, append(b, '\n'), 0o644); err != nil {
		return err
	}
	var lines []string
	for _, spec := range specs {
		lines = append(lines, spec.Query)
	}
	return os.WriteFile(pathTXT, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

func ReadQuerySuite(path string) ([]QuerySpec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var specs []QuerySpec
	if err := json.Unmarshal(b, &specs); err != nil {
		return nil, err
	}
	return specs, nil
}

func topTermsByDF(idx *index.MemoryIndex, limit int) []string {
	type item struct {
		term string
		df   int
	}
	items := make([]item, 0, len(idx.TermStats))
	for term, stat := range idx.TermStats {
		if len(term) < 4 || isStopTerm(term) {
			continue
		}
		minDF := idx.DocCount / 200
		maxDF := idx.DocCount * 4 / 10
		if minDF < 2 {
			minDF = 2
		}
		if stat.DF < minDF || stat.DF > maxDF {
			continue
		}
		items = append(items, item{term: term, df: stat.DF})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].df == items[j].df {
			return items[i].term < items[j].term
		}
		return items[i].df > items[j].df
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.term
	}
	return out
}

func adjacentPairs(docs []index.Document, limit int) [][2]string {
	counts := make(map[[2]string]int)
	for _, doc := range docs {
		tokens := analyzer.Tokenize(doc.Title + " " + doc.Text)
		seen := make(map[[2]string]struct{})
		for i := 0; i+1 < len(tokens); i++ {
			a, b := tokens[i].Term, tokens[i+1].Term
			if len(a) < 4 || len(b) < 4 || a == b || isStopTerm(a) || isStopTerm(b) {
				continue
			}
			seen[[2]string{a, b}] = struct{}{}
		}
		for pair := range seen {
			counts[pair]++
		}
	}
	type item struct {
		pair  [2]string
		count int
	}
	items := make([]item, 0, len(counts))
	for pair, count := range counts {
		if count >= 2 {
			items = append(items, item{pair: pair, count: count})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].pair[0]+items[i].pair[1] < items[j].pair[0]+items[j].pair[1]
		}
		return items[i].count > items[j].count
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([][2]string, len(items))
	for i, it := range items {
		out[i] = it.pair
	}
	return out
}

func countType(specs []QuerySpec, typ string) int {
	n := 0
	for _, spec := range specs {
		if spec.OperatorType == typ {
			n++
		}
	}
	return n
}

func hasEnoughTypes(specs []QuerySpec, perType int, types ...string) bool {
	for _, typ := range types {
		if countType(specs, typ) < perType {
			return false
		}
	}
	return true
}

func isStopTerm(term string) bool {
	switch term {
	case "the", "and", "for", "that", "with", "from", "this", "were", "was", "are", "have", "has", "had", "not", "but", "his", "her", "its", "they", "their", "there", "which", "also", "been", "during", "after", "before", "into", "over", "than", "then", "when", "where", "while", "under", "between", "among", "about", "because":
		return true
	default:
		return false
	}
}
