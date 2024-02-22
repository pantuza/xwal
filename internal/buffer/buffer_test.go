package buffer

import (
	"testing"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"github.com/stretchr/testify/assert"
)

func TestNewInMemoryBuffer(t *testing.T) {
	bufferSizeInMB := 1.0
	nEntries := 10
	buffer := NewInMemoryBuffer(bufferSizeInMB, nEntries)

	assert.Equal(t, bufferSizeInMB, buffer.MaxBufferSizeMB)
	assert.Equal(t, nEntries, buffer.NumberOfEntries)
	assert.Equal(t, 0, buffer.WritesCounter)
	assert.Equal(t, 0.0, buffer.MBCounter)
	assert.Len(t, buffer.Buffer, 0)
}

func TestWriteAndFlush(t *testing.T) {
	buffer := NewInMemoryBuffer(1, 2) // 1 MB buffer size, can hold 2 entries

	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
	}

	// Write the entry to the buffer
	err := buffer.Write(entry)
	assert.NoError(t, err)

	// Attempt to write beyond buffer capacity
	err = buffer.Write(entry)
	assert.NoError(t, err)

	// This write should fail as it exceeds the number of entries limit
	err = buffer.Write(entry)
	assert.EqualError(t, err, ErrorShouldFlushBuffer)

	// Flush the buffer
	flushedData := buffer.Flush()
	assert.Len(t, flushedData, 2)

	// Ensure the buffer is reset after flushing
	assert.Equal(t, 0, buffer.WritesCounter)
	assert.Equal(t, 0.0, buffer.MBCounter)
	assert.Len(t, buffer.Buffer, 0)
}

func TestWriteExceedsMaxBufferSizeMB(t *testing.T) {
	buffer := NewInMemoryBuffer(1, 10) // 1 MB buffer size, but set to hold 10 entries for testing

	// Create an entry that supposedly exceeds the buffer size limit
	// Note: The actual serialized size may not reach 1MB, adjust the Data field as needed to test the limit
	largeEntry := &xwalpb.WALEntry{
		LSN:  2,
		Data: make([]byte, 2*1024*1024), // 2 MB of data

	}
	err := buffer.Write(largeEntry)
	assert.EqualError(t, err, ErrorShouldFlushBuffer, "Expected error when writing entry that exceeds buffer size")
}

func TestReset(t *testing.T) {
	buffer := NewInMemoryBuffer(1, 1)
	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
	}

	// Write an entry and then reset
	err := buffer.Write(entry)
	assert.NoError(t, err)
	buffer.Reset()

	// Check if buffer is correctly reset
	assert.Equal(t, 0, buffer.WritesCounter)
	assert.Equal(t, 0.0, buffer.MBCounter)
	assert.Len(t, buffer.Buffer, 0)
}

func BenchmarkWrite(b *testing.B) {
	buffer := NewInMemoryBuffer(1, 1000) // 1 MB buffer size, can hold 1000 entries

	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Write(entry)
	}
}
