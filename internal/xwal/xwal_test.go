package xwal

import (
	"testing"
	"time"

	"github.com/pantuza/xwal/protobuf/xwalpb"
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

		entry := &xwalpb.WALEntry{
			LSN:  42,
			Data: []byte("fake data"),
			CRC:  42,
		}

		for i := 0; i < 3; i++ {
			if err := wal.Write(entry); err != nil {
				t.Fatal(err)
			}
		}

		// wait for the periodic TestPeriodicFlush to run
		time.Sleep(200 * time.Millisecond)

		entries, err := wal.Replay()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(entries))
		}
	})
}
