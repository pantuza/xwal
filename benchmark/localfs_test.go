package benchmark

import (
	"testing"

	"github.com/pantuza/xwal/internal/xwal"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func BenchmarkLocalFSReplay(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cfg := xwal.NewXWALConfig("")
		cfg.BufferSize = 1
		cfg.BufferEntriesLength = 5
		wal, err := xwal.NewXWAL(cfg)
		if err != nil {
			b.Fatal(err)
		}

		data := []byte("fake data")

		for i := 0; i < 12; i++ {
			if err := wal.Write(data); err != nil {
				b.Fatal(err)
			}
		}

		err = wal.Replay(func(entries []*xwalpb.WALEntry) error {
			return nil
		}, 5, false)
		if err != nil {
			b.Fatal(err)
		}
	}
}
