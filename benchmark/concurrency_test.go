package benchmark

import (
	"testing"

	"github.com/pantuza/xwal/internal/xwal"
	"github.com/stretchr/testify/assert"
)

// Tests multiple functions calling wal.Write concurrently
func BenchmarkConcurrentWrites(b *testing.B) {
	cfg := xwal.NewXWALConfig("")
	xwal, err := xwal.NewXWAL(cfg)
	assert.NoError(b, err)
	defer xwal.Close()

	entry := []byte(`{"name": "John", "age": 42, "city": "Belo Horizonte"}`) // any []byte fake data

	// Run the benchmark
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := xwal.Write(entry)
			assert.NoError(b, err)
		}
	})
}
