package xwal

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"github.com/stretchr/testify/assert"
)

// helper function to simply simulate a long string
func generateRandomString(n int) (string, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

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
}

func TestReplayBackwardsOnXWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	cfg.FlushFrequency = 10 * time.Millisecond
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf("fake data %d", i))
		err = wal.Write(data)
		assert.NoError(t, err)
	}

	time.Sleep(30 * time.Millisecond) // wait for the periodic flush to run
	entriesReaded := make([]*xwalpb.WALEntry, 0, 3)

	err = wal.Replay(func(entries []*xwalpb.WALEntry) error {
		entriesReaded = append(entriesReaded, entries...)
		return nil
	}, 1, true)

	assert.NoError(t, err)
	assert.Equal(t, 3, len(entriesReaded))
	assert.Equal(t, "fake data 2", string(entriesReaded[0].Data))
}

func TestReplayFromRangeOnXWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	cfg.BackendConfig.LocalFS.SegmentsFileSize = 1
	cfg.FlushFrequency = 10 * time.Millisecond
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	for i := 0; i < 20; i++ { // 20 entries makes sure we have more than 5 segments files
		randomString, err := generateRandomString(1024 * 700) // 700 Kb
		assert.NoError(t, err)

		err = wal.Write([]byte(randomString))
		assert.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond) // wait for the periodic flush to run
	expectedEntries := 6
	entriesReaded := make([]*xwalpb.WALEntry, 0, expectedEntries)

	err = wal.ReplayFromRange(func(entries []*xwalpb.WALEntry) error {
		entriesReaded = append(entriesReaded, entries...)
		return nil
	}, 1, false, 3, 5) // replay from segment file 3 to 5

	assert.NoError(t, err)
	assert.Equal(t, expectedEntries, len(entriesReaded))
}

func TestReplayFromRangeWithInvalideRangeOnXWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	err = wal.ReplayFromRange(func(entries []*xwalpb.WALEntry) error {
		return nil
	}, 1, false, 5, 10)

	assert.Error(t, err)
}

func TestReplayFromRangeBackwardsOnXWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	assert.NoError(t, err)

	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	cfg.BackendConfig.LocalFS.SegmentsFileSize = 1
	cfg.FlushFrequency = 10 * time.Millisecond
	wal, err := NewXWAL(cfg)
	assert.NoError(t, err)

	var expectedString string
	for i := 0; i < 20; i++ { // 20 entries makes sure we have more than 5 segments files
		randomString, err := generateRandomString(1024 * 700) // 700 Kb
		assert.NoError(t, err)

		// save the 11th entry to compare later. It will be the first entry to be replayed
		if i == 11 {
			expectedString = randomString
		}

		err = wal.Write([]byte(randomString))
		assert.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond) // wait for the periodic flush to run
	expectedEntries := 6
	entriesReaded := make([]*xwalpb.WALEntry, 0, expectedEntries)

	err = wal.ReplayFromRange(func(entries []*xwalpb.WALEntry) error {
		entriesReaded = append(entriesReaded, entries...)
		return nil
	}, 1, true, 3, 5) // replay from segment file 3 to 5

	assert.NoError(t, err)
	assert.Equal(t, expectedEntries, len(entriesReaded))
	assert.Equal(t, expectedString, string(entriesReaded[0].Data))
}
