package app

import (
	"context"
	"fmt"
	"jokedb/intetnal/compute/analyzer"
	"jokedb/intetnal/logger"
	"jokedb/intetnal/storage/engine"
)

const (
	Name      = "jokedb"
	Addr      = "127.0.0.1:3002"
	MaxConn   = 100
	ConfigPah = "./config/app.yaml"
)

type Processor interface {
	ParseQuery(q string) (analyzer.Action, error)
}

type Storage interface {
	Put(ctx context.Context, kv engine.KV) error
	Get(ctx context.Context, kv engine.KV) (string, error)
	Del(ctx context.Context, kv engine.KV) error
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
		err = a.storage.Put(ctx, actionType.KV)
		if err != nil {
			err = fmt.Errorf("SET query :%w", err)
		} else {
			result = "SET ok"
		}
	case engine.GET:
		result, err = a.storage.Get(ctx, actionType.KV)
		if err != nil {
			err = fmt.Errorf("GET query :%w", err)
		}
	case engine.DEL:
		err = a.storage.Del(ctx, actionType.KV)
		if err != nil {
			err = fmt.Errorf("DEL query :%w", err)
		} else {
			result = "DEL ok"
		}
	}

	return result, err
}

func (a App) Handle(ctx context.Context, s string) string {
	logger.L().Infof("handler get command: %s", s)
	res, err := a.DoRawCommand(ctx, s)
	if err != nil {
		logger.L().Error(err)
	}
	resp := response(err, res)
	logger.L().Infof("handler response: %s", resp)
	return resp
}

func response(err error, res string) string {
	if err != nil {
		return err.Error()
	}
	return res
}
