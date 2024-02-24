package localfs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

func setupLocalFSWALBackend() (*LocalFSWALBackend, string) {
	// Create a temporary directory for WAL files.
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		panic(err)
	}

	cfg := LocalFSConfig{
		DirPath:      dir,
		SegmentsSize: 1,
		FileSize:     1,
	}

	wal := NewLocalFSWALBackend(&cfg)
	err = wal.Open()
	if err != nil {
		panic(err)
	}

	return wal, dir
}

func TestOpen(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()

	// Test opening the WAL.
	err := wal.Open()
	assert.NoError(t, err)

	// Verify the directory was created.
	_, err = os.Stat(dir)
	assert.NoError(t, err)
}

func TestWrite(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()
	err := wal.Open()
	assert.NoError(t, err)

	// Create a sample WALEntry to write.
	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
		CRC:  1,
	}
	entries := []*xwalpb.WALEntry{entry}

	// Test writing the entry to WAL.
	err = wal.Write(entries)
	assert.NoError(t, err)

	// Verify the entry was written correctly.
	marshalledData, err := proto.Marshal(entry)
	assert.NoError(t, err)

	// expected := len(lenBuf) + len(marshalledData)
	// lenBuf := proto.EncodeVarint(uint64(len(marshalledData)))

	filename := fmt.Sprintf(LFSWALSegmentFileFormat, 0) // Since it's the first and only entry, index should be 0.
	filepath := filepath.Join(dir, filename)
	data, err := os.ReadFile(filepath)
	assert.NoError(t, err)

	entryFromFile := new(xwalpb.WALEntry)
	err = protodelim.UnmarshalFrom(bytes.NewBuffer(data), entryFromFile)
	assert.NoError(t, err)

	marshalledFromFile, err := proto.Marshal(entryFromFile)
	assert.NoError(t, err)

	assert.Equal(t, marshalledData, marshalledFromFile, "The data written to the file should match the expected entry data.")
}

func TestReplay(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()
	err := wal.Open()
	assert.NoError(t, err)

	// Create a sample WALEntry to write.
	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
		CRC:  1,
	}
	entries := []*xwalpb.WALEntry{entry}

	// Test writing the entry to WAL.
	err = wal.Write(entries)
	assert.NoError(t, err)

	// Test replaying the WAL.
	replayedEntries, err := wal.Replay()
	assert.NoError(t, err)

	// Verify the replayed entries are correct.
	assert.Equal(t, len(entries), len(replayedEntries), "The replayed entries length should match the written entries length.")
}
