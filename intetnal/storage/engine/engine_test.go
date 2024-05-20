package engine_test

import (
	"context"
	"jokedb/intetnal/storage/engine"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngine(t *testing.T) {
	ctx := context.Background()
	engn := engine.New()

	t.Run("upsert", func(t *testing.T) {
		t.Run("ok", func(t *testing.T) {
			kv := engine.KV{
				Key:   "key1",
				Value: "value",
			}

			err := engn.Upsert(ctx, kv)
			require.NoError(t, err)
		})
		t.Run("err", func(t *testing.T) {
			ctxx, cancel := context.WithCancel(ctx)
			kv := engine.KV{
				Key:   "key1",
				Value: "value",
			}
			cancel()
			err := engn.Upsert(ctxx, kv)
			require.Error(t, err)
		})
		t.Run("many_keys", func(t *testing.T) {
			kvs := []engine.KV{
				{
					Key:   "key1",
					Value: "value",
				},
				{
					Key:   "key2",
					Value: "value",
				},
				{
					Key:   "key3",
					Value: "value",
				},
			}

			for _, kv := range kvs {
				err := engn.Upsert(ctx, kv)
				require.NoError(t, err)
			}

			gotVals := make([]engine.KV, len(kvs))
			for i, kv := range kvs {
				v, err := engn.Get(ctx, kv.Key)
				require.NoError(t, err)
				gotVals[i] = engine.KV{
					Key:   kv.Key,
					Value: v,
				}
			}

			require.EqualValues(t, kvs, gotVals)
		})
		t.Run("duplicated_key", func(t *testing.T) {
			kvs := []engine.KV{
				{
					Key:   "key1",
					Value: "value1",
				},
				{
					Key:   "key1",
					Value: "value2",
				},
			}

			for _, kv := range kvs {
				err := engn.Upsert(ctx, kv)
				require.NoError(t, err)
			}

			got, err := engn.Get(ctx, "key1")
			require.NoError(t, err)

			require.Equal(t, "value2", got)
		})
	})

	t.Run("get", func(t *testing.T) {
		t.Run("ok", func(t *testing.T) {
			kv := engine.KV{
				Key:   "key1",
				Value: "value",
			}

			err := engn.Upsert(ctx, kv)
			require.NoError(t, err)

			got, err := engn.Get(ctx, kv.Key)
			require.NoError(t, err)
			require.Equal(t, kv.Value, got)
		})
		t.Run("err_not_found_key", func(t *testing.T) {
			_, err := engn.Get(ctx, "key")
			require.ErrorIs(t, err, engine.ErrNoKey)
		})
	})

	t.Run("del", func(t *testing.T) {
		t.Run("ok", func(t *testing.T) {
			kv := engine.KV{
				Key:   "key1",
				Value: "value",
			}

			err := engn.Upsert(ctx, kv)
			require.NoError(t, err)

			err = engn.Del(ctx, kv.Key)
			require.NoError(t, err)

			_, err = engn.Get(ctx, kv.Key)
			require.ErrorIs(t, err, engine.ErrNoKey)
		})
	})
}
