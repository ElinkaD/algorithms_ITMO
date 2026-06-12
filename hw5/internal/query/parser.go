package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"hw5/internal/analyzer"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokTerm
	tokPhrase
	tokAnd
	tokOr
	tokNot
	tokAdj
	tokNear
	tokLParen
	tokRParen
)

type qtoken struct {
	kind tokenKind
	text string
	k    int
}

func Parse(input string) (Node, error) {
	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := parser{tokens: tokens}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("unexpected token %q", p.peek().text)
	}
	return node, nil
}

type parser struct {
	tokens []qtoken
	pos    int
}

func (p *parser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	children := []Node{left}
	for p.match(tokOr) {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		children = append(children, right)
	}
	if len(children) == 1 {
		return left, nil
	}
	return OrNode{Children: children}, nil
}

func (p *parser) parseAnd() (Node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	children := []Node{left}
	for p.match(tokAnd) {
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		children = append(children, right)
	}
	if len(children) == 1 {
		return left, nil
	}
	return AndNode{Children: children}, nil
}

func (p *parser) parseNot() (Node, error) {
	if p.match(tokNot) {
		child, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return NotNode{Child: child}, nil
	}
	return p.parseProximity()
}

func (p *parser) parseProximity() (Node, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		if p.match(tokAdj) {
			right, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			left = AdjNode{Left: left, Right: right}
			continue
		}
		if p.peek().kind == tokNear {
			k := p.next().k
			right, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			left = NearNode{Left: left, Right: right, K: k}
			continue
		}
		return left, nil
	}
}

func (p *parser) parsePrimary() (Node, error) {
	tok := p.next()
	switch tok.kind {
	case tokTerm:
		terms := analyzer.Tokenize(tok.text)
		if len(terms) == 0 {
			return nil, fmt.Errorf("empty term")
		}
		return TermNode{Term: terms[0].Term}, nil
	case tokPhrase:
		tokens := analyzer.Tokenize(tok.text)
		if len(tokens) == 0 {
			return nil, fmt.Errorf("empty phrase")
		}
		terms := make([]string, len(tokens))
		for i, t := range tokens {
			terms[i] = t.Term
		}
		return PhraseNode{Terms: terms}, nil
	case tokLParen:
		node, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if !p.match(tokRParen) {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		return node, nil
	default:
		return nil, fmt.Errorf("expected term, phrase or (, got %q", tok.text)
	}
}

func (p *parser) peek() qtoken {
	if p.pos >= len(p.tokens) {
		return qtoken{kind: tokEOF}
	}
	return p.tokens[p.pos]
}

func (p *parser) next() qtoken {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *parser) match(kind tokenKind) bool {
	if p.peek().kind != kind {
		return false
	}
	p.pos++
	return true
}

func lex(input string) ([]qtoken, error) {
	var tokens []qtoken
	for i := 0; i < len(input); {
		r := rune(input[i])
		if unicode.IsSpace(r) {
			i++
			continue
		}
		switch input[i] {
		case '(':
			tokens = append(tokens, qtoken{kind: tokLParen, text: "("})
			i++
		case ')':
			tokens = append(tokens, qtoken{kind: tokRParen, text: ")"})
			i++
		case '"':
			j := i + 1
			for j < len(input) && input[j] != '"' {
				j++
			}
			if j >= len(input) {
				return nil, fmt.Errorf("unterminated phrase")
			}
			tokens = append(tokens, qtoken{kind: tokPhrase, text: input[i+1 : j]})
			i = j + 1
		default:
			j := i
			for j < len(input) && !unicode.IsSpace(rune(input[j])) && input[j] != '(' && input[j] != ')' {
				j++
			}
			word := input[i:j]
			upper := strings.ToUpper(word)
			switch {
			case upper == "AND":
				tokens = append(tokens, qtoken{kind: tokAnd, text: word})
			case upper == "OR":
				tokens = append(tokens, qtoken{kind: tokOr, text: word})
			case upper == "NOT":
				tokens = append(tokens, qtoken{kind: tokNot, text: word})
			case upper == "ADJ":
				tokens = append(tokens, qtoken{kind: tokAdj, text: word})
			case strings.HasPrefix(upper, "NEAR/"):
				k, err := strconv.Atoi(word[len("NEAR/"):])
				if err != nil || k < 1 {
					return nil, fmt.Errorf("bad NEAR distance: %q", word)
				}
				tokens = append(tokens, qtoken{kind: tokNear, text: word, k: k})
			default:
				tokens = append(tokens, qtoken{kind: tokTerm, text: word})
			}
			i = j
		}
	}
	tokens = append(tokens, qtoken{kind: tokEOF})
	return tokens, nil
}
