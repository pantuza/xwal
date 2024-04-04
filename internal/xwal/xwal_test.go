package xwal

import (
	"os"
	"testing"
	"time"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"github.com/stretchr/testify/assert"
)

func TestPeriodicFlush(t *testing.T) {
	t.Run("PeriodicFlush", func(t *testing.T) {
		t.Parallel() // This test is now Parallel

		// Create a temporary directory for WAL files.
		dir, err := os.MkdirTemp("", "wal_test")
		if err != nil {
			panic(err)
		}
		cfg := NewXWALConfig("")
		cfg.BackendConfig.LocalFS.DirPath = dir
		cfg.BufferSize = 1
		cfg.BufferEntriesLength = 5
		cfg.FlushFrequency = 100 * time.Millisecond
		wal, err := NewXWAL(cfg)
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("fake data")

		for i := 0; i < 3; i++ {
			if err := wal.Write(data); err != nil {
				t.Fatal(err)
			}
		}

		// wait for the periodic TestPeriodicFlush to run
		time.Sleep(200 * time.Millisecond)

		entriesReaded := make([]*xwalpb.WALEntry, 0, 3)

		err = wal.Replay(func(entries []*xwalpb.WALEntry) error {
			entriesReaded = append(entriesReaded, entries...)
			return nil
		}, 1, false)

		assert.NoError(t, err)
		assert.Equal(t, 3, len(entriesReaded))
	})
}

func TestWriteWhenTheWalIsAlreadyClosed(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	err = wal.Close()
	assert.NoError(t, err)

	data := []byte("fake data")
	err = wal.Write(data)
	assert.Error(t, err)
}

func TestReplayWhenTheWalIsAlreadyClosed(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	err = wal.Close()
	assert.NoError(t, err)

	err = wal.Replay(func(entries []*xwalpb.WALEntry) error {
		return nil
	}, 1, false)
	assert.Error(t, err)
}

func TestWriteOnXWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	data := []byte("fake data")
	err = wal.Write(data)
	assert.NoError(t, err)

	err = wal.Close()
	assert.NoError(t, err)
}

func TestReplayOnXWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	cfg.FlushFrequency = 50 * time.Millisecond
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	data := []byte("fake data")
	err = wal.Write(data)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond) // wait for the periodic flush to run
	entriesReaded := make([]*xwalpb.WALEntry, 0, 1)

	err = wal.Replay(func(entries []*xwalpb.WALEntry) error {
		entriesReaded = append(entriesReaded, entries...)
		return nil
	}, 1, false)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(entriesReaded))

	err = wal.Close()
	assert.NoError(t, err)
}
