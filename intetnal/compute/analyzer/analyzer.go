package analyzer

import (
	"errors"
	"jokedb/intetnal/storage/engine"
)

const (
	MinTokens = 2
	MaxTokens = 3
)

type Action struct {
	Type engine.ActionType
	engine.KV
}

type Analyzer struct{}

func New() *Analyzer {
	return &Analyzer{}
}

func (al Analyzer) Analyze(tokens []string) (Action, error) {
	a := Action{}
	types := map[string]engine.ActionType{
		"SET": engine.SET,
		"GET": engine.GET,
		"DEL": engine.DEL,
	}

	if len(tokens) < MinTokens {
		return a, errors.New("tokens size less than 2")
	}

	t, ok := types[tokens[0]]
	if !ok {
		return a, errors.New("unkown command")
	}

	a.Type = t
	a.Key = tokens[1]
	if t == engine.SET {
		if len(tokens) < MaxTokens {
			return a, errors.New("no value set for key")
		}
		a.Value = tokens[2]
	}

	return a, nil
}
