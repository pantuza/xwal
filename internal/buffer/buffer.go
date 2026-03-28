package buffer

import (
	"errors"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"google.golang.org/protobuf/proto"
)

// ErrShouldFlushBuffer is returned by Write when the in-memory buffer must be
// flushed before accepting another entry (size or entry-count limit). Callers
// should flush and retry; xwal uses errors.Is to detect this sentinel.
var ErrShouldFlushBuffer = errors.New("buffer should be flushed before writing")

type InMemoryBuffer struct {
	MaxBufferSizeMB float64
	NumberOfEntries int
	WritesCounter   int
	MBCounter       float64
	Buffer          []*xwalpb.WALEntry
}

func NewInMemoryBuffer(bufferSizeInMB float64, nEntries int) *InMemoryBuffer {
	return &InMemoryBuffer{
		MaxBufferSizeMB: bufferSizeInMB,
		NumberOfEntries: nEntries,
		WritesCounter:   0,
		MBCounter:       0.0,
		Buffer:          make([]*xwalpb.WALEntry, 0, nEntries),
	}
}

func (b *InMemoryBuffer) Write(entry *xwalpb.WALEntry) error {
	entry_size := float64(proto.Size(entry)) / 1024 / 1024

	if b.MBCounter+entry_size > b.MaxBufferSizeMB || b.WritesCounter >= b.NumberOfEntries {
		return ErrShouldFlushBuffer
	}

	b.Buffer = append(b.Buffer, entry)
	b.WritesCounter++
	b.MBCounter += entry_size

	return nil
}

func (b *InMemoryBuffer) Flush() []*xwalpb.WALEntry {
	data := make([]*xwalpb.WALEntry, 0, b.WritesCounter)
	data = append(data, b.Buffer...)

	b.Reset()
	return data
}

func (b *InMemoryBuffer) Reset() {
	b.WritesCounter = 0
	b.MBCounter = 0
	b.Buffer = make([]*xwalpb.WALEntry, 0, b.NumberOfEntries)
}

// Stats returns the current entry count and approximate buffered payload size in bytes
// derived from the same accounting used for flush thresholds (protobuf size, MB float).
func (b *InMemoryBuffer) Stats() (entries int, approxSizeBytes int64) {
	return b.WritesCounter, int64(b.MBCounter * 1024 * 1024)
}
