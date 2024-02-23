package benchmark

import (
	"testing"

	"github.com/pantuza/xwal/internal/buffer"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func BenchmarkWrite(b *testing.B) {
	buffer := buffer.NewInMemoryBuffer(1, 1000) // 1 MB buffer size, can hold 1000 entries

	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buffer.Write(entry)
	}
}
