package storage

import (
	"context"
	"errors"
	"fmt"
	"jokedb/intetnal/storage/engine"
	"jokedb/intetnal/syncutils"
	"jokedb/intetnal/wal"
	"sync"
	"sync/atomic"
	"time"
)

const pendingSize = 32 * 1024

type Storage struct {
	engine               *engine.Engine
	wal                  *wal.WAL
	pending              chan PendingLog
	flushingBatchSize    uint32
	flushingBatchTimeout time.Duration
	isStop               atomic.Bool
	mu                   sync.RWMutex
}

func New(
	engine *engine.Engine, wal *wal.WAL,
	flushingBatchSize uint32,
	flushingBatchTimeout time.Duration,
) (*Storage, error) {
	s := &Storage{
		engine:               engine,
		wal:                  wal,
		pending:              make(chan PendingLog, pendingSize),
		flushingBatchSize:    flushingBatchSize,
		flushingBatchTimeout: flushingBatchTimeout,
	}

	if err := s.recovery(); err != nil {
		return nil, err
	}

	go s.run()

	return s, nil
}

func (s *Storage) Put(ctx context.Context, kv engine.KV) error {
	if err := s.pendingWrite(
		ctx,
		wal.LogData{
			Action: engine.SET,
			Key:    kv.Key,
			Value:  kv.Value,
		}); err != nil {
		return err
	}

	return s.engine.Upsert(ctx, kv)
}

func (s *Storage) Del(ctx context.Context, kv engine.KV) error {
	if err := s.pendingWrite(
		ctx,
		wal.LogData{
			Action: engine.DEL,
			Key:    kv.Key,
			Value:  kv.Value,
		}); err != nil {
		return err
	}
	return s.engine.Del(ctx, kv.Key)
}

func (s *Storage) Get(ctx context.Context, kv engine.KV) (string, error) {
	return s.engine.Get(ctx, kv.Key)
}

func (s *Storage) pendingWrite(ctx context.Context, log wal.LogData) error {
	if s.wal == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isStop.Load() {
		return nil
	}

	p := syncutils.NewPromise[error]()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.pending <- PendingLog{
		LogData: log,
		promise: p,
	}:
	default:
		p.Set(fmt.Errorf("канал для приема событий заполнен. Размер канала: %d", s.flushingBatchSize))
	}

	future := p.GetFuture()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return future.Get()
	}
}

func (s *Storage) run() {
	batch, promises := s.makeBatches()
	ticker := time.NewTicker(s.flushingBatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.flushBatch(batch, promises)
			batch, promises = s.makeBatches()
		case v, ok := <-s.pending:
			if ok {
				batch = append(batch, v.LogData)
				promises = append(promises, v.promise)
				if uint32(len(batch)) == s.flushingBatchSize {
					s.flushBatch(batch, promises)
					batch, promises = s.makeBatches()
				}
			} else {
				s.flushBatch(batch, promises)
				return
			}
		}
	}
}

func (s *Storage) makeBatches() ([]wal.LogData, []syncutils.Promise[error]) {
	batch := make([]wal.LogData, 0, s.flushingBatchSize)
	promises := make([]syncutils.Promise[error], 0, s.flushingBatchSize)
	return batch, promises
}

func (s *Storage) flushBatch(batch []wal.LogData, promises []syncutils.Promise[error]) {
	if len(batch) == 0 {
		return
	}
	err := s.wal.Write(batch)
	for _, p := range promises {
		p.Set(err)
	}
}

type PendingLog struct {
	wal.LogData
	promise syncutils.Promise[error]
}

func (s *Storage) recovery() error {
	if s.wal == nil {
		return nil
	}

	ctx := context.Background()
	logs, err := s.wal.ReadSegments()
	if err != nil {
		return err
	}

	s.engine.Flush()

	for _, log := range logs {
		if s.isStop.Load() {
			return nil
		}
		kv := engine.KV{Key: log.Key, Value: log.Value}
		switch log.Action {
		case engine.SET:
			if err = s.engine.Upsert(ctx, kv); err != nil {
				return err
			}
		case engine.DEL:
			if err = s.engine.Del(ctx, kv.Key); err != nil {
				return err
			}
		case engine.GET:
		default:
			return errors.New("не известный тип лога")
		}
	}

	return nil
}

func (s *Storage) Close() {
	s.mu.Lock()
	s.isStop.Store(true)
	s.mu.Unlock()

	close(s.pending)
}
