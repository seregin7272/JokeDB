package wal_test

import (
	"jokedb/intetnal/storage/engine"
	"jokedb/intetnal/wal"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWAL(t *testing.T) {
	dirPath := "test_data"

	logs := []wal.LogData{
		{
			Action: engine.SET,
			Key:    "key_2",
			Value:  "value_2",
		},
		{
			Action: engine.SET,
			Key:    "key_1",
			Value:  "value_2",
		},
		{
			Action: engine.DEL,
			Key:    "key_1",
			Value:  "value_2",
		},
	}

	t.Run("simple", func(t *testing.T) {
		walLog, err := wal.Open(wal.WithDirPath(dirPath))
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = walLog.Close()
			_ = os.RemoveAll(dirPath)
		})

		err = walLog.Write(logs)
		require.NoError(t, err)
		err = walLog.Write(logs)
		require.NoError(t, err)

		err = walLog.NewActiveSegment()
		require.NoError(t, err)

		err = walLog.Write(logs)
		require.NoError(t, err)
		err = walLog.Write(logs)
		require.NoError(t, err)

		var wantLogs []wal.LogData
		wantLogs = append(wantLogs, logs...)
		wantLogs = append(wantLogs, logs...)
		wantLogs = append(wantLogs, logs...)
		wantLogs = append(wantLogs, logs...)

		gotLogs, err := walLog.ReadSegments()
		require.NoError(t, err)

		require.Equal(t, wantLogs, gotLogs)
	})

	t.Run("rotate_segment", func(t *testing.T) {
		walLog, err := wal.Open(wal.WithDirPath(dirPath), wal.WithMaxSizeSegment(300))
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = walLog.Close()
			_ = os.RemoveAll(dirPath)
		})

		err = walLog.Write(logs)
		require.NoError(t, err)

		require.Equal(t, dirPath+"/000000001.seg", walLog.ActiveSegment().FileName())

		err = walLog.Write(logs)
		require.NoError(t, err)

		err = walLog.Write(logs)
		require.NoError(t, err)

		require.Equal(t, dirPath+"/000000002.seg", walLog.ActiveSegment().FileName())
	})
}
