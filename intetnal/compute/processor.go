package compute

import (
	"jokedb/intetnal/compute/analyzer"
	"jokedb/intetnal/compute/parser"
)

type Parser interface {
	Tokenization(in string) (tokens []string, err error)
}

type Analyzer interface {
	Analyze(tokens []string) (analyzer.Action, error)
}

type Processor struct {
	parser   Parser
	analyzer Analyzer
}

func New() *Processor {
	return &Processor{
		parser:   &parser.Parser{},
		analyzer: &analyzer.Analyzer{},
	}
}

func (c Processor) ParseQuery(q string) (analyzer.Action, error) {
	tokens, err := c.parser.Tokenization(q)
	if err != nil {
		return analyzer.Action{}, err
	}

	return c.analyzer.Analyze(tokens)
}
