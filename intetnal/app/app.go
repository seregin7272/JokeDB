package app

import (
	"context"
	"fmt"
	"jokedb/intetnal/compute/analyzer"
	"jokedb/intetnal/storage/engine"
)

type Processor interface {
	ParseQuery(q string) (analyzer.Action, error)
}

type Storage interface {
	Upsert(ctx context.Context, kv engine.KV) error
	Get(ctx context.Context, k string) (string, error)
	Del(ctx context.Context, k string) error
}

type App struct {
	processor Processor
	storage   Storage
}

func New(p Processor, s Storage) *App {
	return &App{
		processor: p,
		storage:   s,
	}
}

func (a App) DoRawCommand(ctx context.Context, c string) (string, error) {
	actionType, err := a.processor.ParseQuery(c)
	if err != nil {
		return "", fmt.Errorf("parse query :%w", err)
	}

	var result string
	switch actionType.Type {
	case engine.SET:
		err = a.storage.Upsert(ctx, actionType.KV)
		if err != nil {
			err = fmt.Errorf("SET query :%w", err)
		} else {
			result = "SET ok"
		}
	case engine.GET:
		result, err = a.storage.Get(ctx, actionType.Key)
		if err != nil {
			err = fmt.Errorf("GET query :%w", err)
		}
	case engine.DEL:
		err = a.storage.Del(ctx, actionType.Key)
		if err != nil {
			err = fmt.Errorf("DEL query :%w", err)
		} else {
			result = "DEL ok"
		}
	}

	return result, err
}
