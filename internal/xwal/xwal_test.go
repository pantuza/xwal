package xwal

import (
	"testing"
	"time"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"github.com/stretchr/testify/assert"
)

func TestPeriodicFlush(t *testing.T) {
	t.Run("PeriodicFlush", func(t *testing.T) {
		t.Parallel() // This test is now Parallel

		cfg := NewXWALConfig("")
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
		}, 1)

		assert.NoError(t, err)
		assert.Equal(t, 3, len(entriesReaded))
	})
}
