package buffer

import (
	"bytes"
	"errors"
)

const (
	ErrorShouldFlushBuffer = "Buffer should be flushed before writing"
)

type InMemoryBuffer struct {
	BufferSizeMB    int
	NumberOfEntries int
	WritesCounter   int
	Buffer          bytes.Buffer
}

func NewInMemoryBuffer(bufferSizeInMB, nEntries int) *InMemoryBuffer {
	return &InMemoryBuffer{
		BufferSizeMB:    bufferSizeInMB,
		NumberOfEntries: nEntries,
		WritesCounter:   0,
		Buffer:          bytes.Buffer{},
	}
}

func (b *InMemoryBuffer) Write(p []byte) (n int, err error) {
	if b.Buffer.Len() > b.BufferSizeMB || b.WritesCounter > b.NumberOfEntries {
		return 0, errors.New(ErrorShouldFlushBuffer)
	}

	return b.Buffer.Write(p)
}

func (b *InMemoryBuffer) Flush() []byte {
	data := b.Buffer.Bytes()
	b.Buffer.Reset()

	return data
}
