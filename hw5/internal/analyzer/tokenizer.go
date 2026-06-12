package analyzer

import (
	"strings"
	"unicode"
)

type Token struct {
	Term     string
	Position int
}

type Filter interface {
	Keep(term string) bool
}

type Analyzer struct {
	Filter Filter
}

func (a Analyzer) Analyze(text string) []Token {
	var tokens []Token
	var b strings.Builder
	position := 0
	flush := func() {
		if b.Len() == 0 {
			return
		}
		term := b.String()
		b.Reset()
		if a.Filter != nil && !a.Filter.Keep(term) {
			return
		}
		tokens = append(tokens, Token{Term: term, Position: position})
		position++
	}
	for _, r := range strings.ToLower(text) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return tokens
}

func Tokenize(text string) []Token {
	return Analyzer{}.Analyze(text)
}
