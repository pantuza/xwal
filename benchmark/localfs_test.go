package benchmark

import (
	"testing"

	"github.com/pantuza/xwal/internal/xwal"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func BenchmarkLocalFSReplay(b *testing.B) {
	cfg := xwal.NewXWALConfig("")
	cfg.BufferSize = 1
	cfg.BufferEntriesLength = 5
	wal, err := xwal.NewXWAL(cfg)
	if err != nil {
		b.Fatal(err)
	}

	entry := &xwalpb.WALEntry{
		LSN:  42,
		Data: []byte("fake data"),
		CRC:  42,
	}

	for i := 0; i < 12; i++ {
		if err := wal.Write(entry); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := wal.Replay(func(entries []*xwalpb.WALEntry) error {
			return nil
		}, 5)
		if err != nil {
			b.Fatal(err)
		}
	}
}
