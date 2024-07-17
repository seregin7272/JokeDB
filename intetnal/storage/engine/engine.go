package engine

import (
	"context"
	"errors"
	"sync"
)

type ActionType int8

const (
	SET ActionType = iota + 1
	GET
	DEL
)

type KV struct {
	Key   string
	Value string
}

var ErrNoKey = errors.New("no key")

type Engine struct {
	mu      sync.RWMutex
	storage map[string]string
}

func New() *Engine {
	return &Engine{
		mu:      sync.RWMutex{},
		storage: map[string]string{},
	}
}

func (e *Engine) Upsert(ctx context.Context, kv KV) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.storage[kv.Key] = kv.Value

	return nil
}

func (e *Engine) Get(ctx context.Context, k string) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	v, ok := e.storage[k]
	if !ok {
		return "", ErrNoKey
	}
	return v, nil
}

func (e *Engine) Del(ctx context.Context, k string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.storage, k)

	return nil
}

func (e *Engine) Flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.storage = map[string]string{}
}
