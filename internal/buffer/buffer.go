package buffer

import (
	"errors"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"google.golang.org/protobuf/proto"
)

const (
	ErrorShouldFlushBuffer = "Buffer should be flushed before writing"
)

type InMemoryBuffer struct {
	MaxBufferSizeMB int
	NumberOfEntries int
	WritesCounter   int
	MBCounter       int
	Buffer          []*xwalpb.WALEntry
}

func NewInMemoryBuffer(bufferSizeInMB, nEntries int) *InMemoryBuffer {
	return &InMemoryBuffer{
		MaxBufferSizeMB: bufferSizeInMB,
		NumberOfEntries: nEntries,
		WritesCounter:   0,
		MBCounter:       0,
		Buffer:          make([]*xwalpb.WALEntry, 0, nEntries),
	}
}

func (b *InMemoryBuffer) Write(entry *xwalpb.WALEntry) error {
	entry_size := proto.Size(entry) / 1024 / 1024

	if b.MBCounter+entry_size > b.MaxBufferSizeMB || b.WritesCounter >= b.NumberOfEntries {
		return errors.New(ErrorShouldFlushBuffer)
	}

	b.Buffer = append(b.Buffer, entry)
	b.WritesCounter++
	b.MBCounter += entry_size

	return nil
}

func (b *InMemoryBuffer) Flush() []*xwalpb.WALEntry {
	data := make([]*xwalpb.WALEntry, 0, b.WritesCounter)
	copy(data, b.Buffer)

	b.Reset()
	return data
}

func (b *InMemoryBuffer) Reset() {
	b.WritesCounter = 0
	b.MBCounter = 0
	b.Buffer = make([]*xwalpb.WALEntry, 0, b.NumberOfEntries)
}
