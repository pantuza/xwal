package benchmark

import (
	"testing"

	"github.com/pantuza/xwal"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func BenchmarkLocalFSReplay(b *testing.B) {
	for range b.N {
		// Close the WAL before the next iteration so Windows can remove b.TempDir() trees
		// (files cannot be deleted while still open).
		func() {
			cfg := xwal.NewXWALConfig("")
			cfg.BackendConfig.LocalFS.DirPath = b.TempDir()
			cfg.BufferSize = 1
			cfg.BufferEntriesLength = 5
			wal, err := xwal.NewXWAL(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = wal.Close() }()

			data := []byte("fake data")

			for range 12 {
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
		}()
	}
}
