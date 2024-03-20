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

	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
		CRC:  1,
	}
	entries := []*xwalpb.WALEntry{entry}

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

	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
		CRC:  1,
	}
	entries := []*xwalpb.WALEntry{entry}

	err = wal.Write(entries)
	assert.NoError(t, err)

	channel := make(chan *xwalpb.WALEntry, 1)
	err = wal.Replay(channel)
	assert.NoError(t, err)
	close(channel)

	replayedEntries := make([]*xwalpb.WALEntry, 0)
	for entry := range channel {
		replayedEntries = append(replayedEntries, entry)
	}

	// Verify the replayed entries are correct.
	assert.Equal(t, len(entries), len(replayedEntries), "The replayed entries length should match the written entries length.")
}

func TestGetSegmentsFilesFromRange(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()
	err := wal.Open()
	assert.NoError(t, err)

	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
		CRC:  1,
	}
	entries := []*xwalpb.WALEntry{entry}

	err = wal.Write(entries)
	assert.NoError(t, err)

	// Test getting the segment files from the range.
	segmentFiles, err := wal.getSegmentsFilesFromRange(0, 0)
	assert.NoError(t, err)

	// Verify the segment files are correct.
	expected := []string{filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, 0))}
	assert.Equal(t, expected, segmentFiles, "The segment files should match the expected segment files.")
}

func TestReadEntriesFromFile(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()
	err := wal.Open()
	assert.NoError(t, err)

	entry := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
		CRC:  1,
	}
	entries := []*xwalpb.WALEntry{entry}

	err = wal.Write(entries)
	assert.NoError(t, err)

	filename := fmt.Sprintf(LFSWALSegmentFileFormat, 0)
	filepath := filepath.Join(wal.cfg.DirPath, filename)
	file, err := os.Open(filepath)
	assert.NoError(t, err)

	readedEntries, err := wal.readEntriesFromFile(file)
	assert.NoError(t, err)

	// Verify the readed entries are correct.
	assert.Equal(t, len(entries), len(readedEntries), "The readed entries should match the written entries.")
}

func TestGetLastSequencyNumber(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()
	err := wal.Open()
	assert.NoError(t, err)

	entry1 := &xwalpb.WALEntry{
		LSN:  1,
		Data: []byte("test data"),
		CRC:  1,
	}
	entry2 := &xwalpb.WALEntry{
		LSN:  2,
		Data: []byte("test data"),
		CRC:  1,
	}
	entries := []*xwalpb.WALEntry{entry1, entry2}

	err = wal.Write(entries)
	assert.NoError(t, err)

	err = wal.getLastLogSequencyNumber()
	assert.NoError(t, err)

	// Verify the last sequence number is correct.
	assert.Equal(t, uint64(2), wal.lastLSN, "The last sequence number should match the expected sequence number.")
}

func TestGetLastSequencyNumberWithNoEntries(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	err := wal.getLastLogSequencyNumber()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), wal.lastLSN, "The last sequence number should be 0 since there is no entries.")
}
