package localfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func setupLocalFSWALBackend() (*LocalFSWALBackend, string) {
	// Create a temporary directory for WAL files.
	dir, err := ioutil.TempDir("", "wal_test")
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
	expected, err := proto.Marshal(entry)
	assert.NoError(t, err)

	filename := fmt.Sprintf(LFSWALSegmentFileFormat, 0) // Since it's the first and only entry, index should be 0.
	filepath := filepath.Join(dir, filename)
	data, err := os.ReadFile(filepath)
	assert.NoError(t, err)
	assert.Equal(t, expected, data[:len(expected)], "The data written to the file should match the expected entry data.")
}