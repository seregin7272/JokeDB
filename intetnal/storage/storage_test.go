package storage_test

import (
	"context"
	"jokedb/intetnal/storage"
	"jokedb/intetnal/storage/engine"
	wallog "jokedb/intetnal/wal"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	t.Parallel()
	e := engine.New()
	batchSize := 3

	t.Run("flushing_by_batch_size", func(t *testing.T) {
		t.Parallel()
		wal, err := wallog.Open(wallog.WithDirPath(t.TempDir()))
		require.NoError(t, err)
		s, err := storage.New(e, wal, uint32(batchSize), 2*time.Second)
		require.NoError(t, err)
		t.Cleanup(s.Close)
		ctx := context.Background()

		wg := sync.WaitGroup{}

		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = s.Put(ctx, engine.KV{Key: "key1", Value: "value1"})
				assert.NoError(t, err)
			}()
		}

		require.Eventually(t, func() bool {
			wg.Wait()
			return true
		}, time.Second, time.Millisecond)
	})
	t.Run("flushing_by_timeout", func(t *testing.T) {
		t.Parallel()
		wal, err := wallog.Open(wallog.WithDirPath(t.TempDir()))
		require.NoError(t, err)
		s, err := storage.New(e, wal, uint32(batchSize), 10*time.Millisecond)
		require.NoError(t, err)
		t.Cleanup(s.Close)
		ctx := context.Background()
		wg := sync.WaitGroup{}

		for i := 0; i < batchSize-1; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = s.Del(ctx, engine.KV{Key: "key1", Value: "value1"})
				assert.NoError(t, err)
			}()
		}

		require.Eventually(t, func() bool {
			wg.Wait()
			return true
		}, time.Second, time.Millisecond)
	})
	t.Run("put_and_del", func(t *testing.T) {
		t.Parallel()
		wal, err := wallog.Open(wallog.WithDirPath(t.TempDir()))
		require.NoError(t, err)
		s, err := storage.New(e, wal, uint32(batchSize), time.Millisecond)
		require.NoError(t, err)
		t.Cleanup(s.Close)
		ctx := context.Background()

		err = s.Put(ctx, engine.KV{Key: "key_123", Value: "value_123"})
		require.NoError(t, err)

		v, err := s.Get(ctx, engine.KV{Key: "key_123"})
		require.NoError(t, err)
		require.Equal(t, "value_123", v)

		err = s.Del(ctx, engine.KV{Key: "key_123"})
		require.NoError(t, err)

		_, err = s.Get(ctx, engine.KV{Key: "key_123"})
		require.ErrorIs(t, err, engine.ErrNoKey)
	})
	t.Run("recovery", func(t *testing.T) {
		t.Parallel()
		wal, err := wallog.Open(wallog.WithDirPath("test_recovery"))
		require.NoError(t, err)
		s, err := storage.New(e, wal, uint32(batchSize), time.Millisecond)
		require.NoError(t, err)
		t.Cleanup(s.Close)
		ctx := context.Background()

		v, _ := s.Get(ctx, engine.KV{Key: "key_321"})
		require.Equal(t, "value_321", v)

		v, _ = s.Get(ctx, engine.KV{Key: "key_322"})
		require.Equal(t, "value_322", v)
	})
}
