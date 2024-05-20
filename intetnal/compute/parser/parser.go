package parser

import (
	"errors"
	"strings"
)

var ErrNotValidSymbol = errors.New("not valid symbol")

type Parser struct{}

func New() *Parser {
	return &Parser{}
}

func (p Parser) Tokenization(in string) ([]string, error) {
	b := strings.Builder{}
	var err error
	var tokens []string

	for _, v := range in {
		switch {
		case v == ' ':
			tokens = append(tokens, b.String())
			b.Reset()

		case isValid(v):
			_, err = b.WriteRune(v)
			if err != nil {
				return nil, err
			}
		default:
			return nil, ErrNotValidSymbol
		}
	}

	return append(tokens, b.String()), nil
}

func isValid(r rune) bool {
	switch {
	case r >= '0' && r <= '9':
		return true
	case r >= 'A' && r <= 'Z':
		return true
	case r >= 'a' && r <= 'z':
		return true
	case r == '*':
		return true
	case r == '/':
		return true
	case r == '_':
		return true
	}

	return false
}
