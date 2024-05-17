package localfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		DirPath:            dir,
		SegmentsFileSizeMB: 1,
		SegmentsDirSizeGB:  1,
	}

	wal := NewLocalFSWALBackend(&cfg)
	err = wal.Open()
	if err != nil {
		panic(err)
	}

	return wal, dir
}

func TestOpen(t *testing.T) {
	_, dir := setupLocalFSWALBackend()

	// Verify the directory was created.
	_, err := os.Stat(dir)
	assert.NoError(t, err)
}

func TestWrite(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()

	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data"), CRC: 1}
	entries := []*xwalpb.WALEntry{entry}

	err := wal.Write(entries)
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

	var err error
	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data")}
	entry.CRC, err = entry.Checksum()
	assert.NoError(t, err)

	entries := []*xwalpb.WALEntry{entry}

	err = wal.Write(entries)
	assert.NoError(t, err)

	channel := make(chan *xwalpb.WALEntry, 1)
	err = wal.Replay(channel, false)
	assert.NoError(t, err)
	close(channel)

	replayedEntries := make([]*xwalpb.WALEntry, 0)
	for entry := range channel {
		replayedEntries = append(replayedEntries, entry)
	}

	// Verify the replayed entries are correct.
	assert.Equal(t, len(entries), len(replayedEntries), "The replayed entries length should match the written entries length.")
}

func TestReplayBackwards(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	var err error
	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data 1")}
	entry1.CRC, err = entry1.Checksum()
	assert.NoError(t, err)

	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("test data 2")}
	entry2.CRC, err = entry2.Checksum()
	assert.NoError(t, err)

	entries := []*xwalpb.WALEntry{entry1, entry2}

	err = wal.Write(entries)
	assert.NoError(t, err)

	channel := make(chan *xwalpb.WALEntry, 2)
	err = wal.Replay(channel, true)
	assert.NoError(t, err)
	close(channel)

	replayedEntries := make([]*xwalpb.WALEntry, 0)
	for entry := range channel {
		replayedEntries = append(replayedEntries, entry)
	}

	assert.Equal(t, len(entries), len(replayedEntries), "The replayed entries length should match the written entries length.")
	assert.True(t, proto.Equal(entry2, replayedEntries[0]), "The replayed entries should match the last written entry because it was replayed backwards.")
}

func TestReplayFromRange(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	var err error
	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data 1")}
	entry1.CRC, err = entry1.Checksum()
	assert.NoError(t, err)

	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("test data 2")}
	entry2.CRC, err = entry2.Checksum()
	assert.NoError(t, err)

	entries := []*xwalpb.WALEntry{entry1, entry2}

	err = wal.Write(entries)
	assert.NoError(t, err)

	// make two copies of the segiment file to test the replay from range
	inputSegmentFile, err := os.OpenFile(filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, 0)), os.O_RDONLY, 0644)
	assert.NoError(t, err)
	defer inputSegmentFile.Close()
	inputContent, err := io.ReadAll(inputSegmentFile)
	assert.NoError(t, err)

	segmentFileCopy1, err := os.Create(filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, 1)))
	assert.NoError(t, err)
	defer segmentFileCopy1.Close()
	segmentFileCopy2, err := os.Create(filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, 2)))
	assert.NoError(t, err)
	defer segmentFileCopy2.Close()

	_, err = segmentFileCopy1.Write(inputContent)
	assert.NoError(t, err)
	_, err = segmentFileCopy2.Write(inputContent)
	assert.NoError(t, err)

	wal.lastSegmentIndex = 2 // set the last segment index to 2 to test the replay from range

	channel := make(chan *xwalpb.WALEntry, 6)
	err = wal.ReplayFromRange(channel, false, 1, 2)
	assert.NoError(t, err)
	close(channel)

	replayedEntries := make([]*xwalpb.WALEntry, 0)
	for entry := range channel {
		replayedEntries = append(replayedEntries, entry)
	}

	// 2*len(entries) because we are replaying the range 1 to 2 that means we have two files with two entries.
	assert.Equal(t, 2*len(entries), len(replayedEntries), "The replayed entries length should match the length of only two segments files.")
}

func TestReplayFromRangeBackwards(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	var err error
	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data 1")}
	entry1.CRC, err = entry1.Checksum()
	assert.NoError(t, err)

	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("test data 2")}
	entry2.CRC, err = entry2.Checksum()
	assert.NoError(t, err)

	entries := []*xwalpb.WALEntry{entry1, entry2}

	err = wal.Write(entries)
	assert.NoError(t, err)

	// make two copies of the segiment file to test the replay from range
	inputSegmentFile, err := os.OpenFile(filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, 0)), os.O_RDONLY, 0644)
	assert.NoError(t, err)
	defer inputSegmentFile.Close()
	inputContent, err := io.ReadAll(inputSegmentFile)
	assert.NoError(t, err)

	segmentFileCopy1, err := os.Create(filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, 1)))
	assert.NoError(t, err)
	defer segmentFileCopy1.Close()
	segmentFileCopy2, err := os.Create(filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, 2)))
	assert.NoError(t, err)
	defer segmentFileCopy2.Close()

	_, err = segmentFileCopy1.Write(inputContent)
	assert.NoError(t, err)
	_, err = segmentFileCopy2.Write(inputContent)
	assert.NoError(t, err)

	wal.lastSegmentIndex = 2 // set the last segment index to 2 to test the replay from range

	channel := make(chan *xwalpb.WALEntry, 6)
	err = wal.ReplayFromRange(channel, true, 0, 1)
	assert.NoError(t, err)
	close(channel)

	replayedEntries := make([]*xwalpb.WALEntry, 0)
	for entry := range channel {
		replayedEntries = append(replayedEntries, entry)
	}

	assert.True(t, proto.Equal(entry2, replayedEntries[0]), "The replayed entries should match the last written entry because it was replayed backwards.")
	// 2*len(entries) because we are replaying the range 1 to 2 that means we have two files with two entries.
	assert.Equal(t, 2*len(entries), len(replayedEntries), "The replayed entries length should match the length of only two segments files.")
}

func TestReplayFrominvalidRange(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	channel := make(chan *xwalpb.WALEntry, 6)
	err := wal.ReplayFromRange(channel, false, 2, 1)
	assert.Error(t, err)
}

func TestGetSegmentsFilesFromRange(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()

	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data"), CRC: 1}
	entries := []*xwalpb.WALEntry{entry}

	err := wal.Write(entries)
	assert.NoError(t, err)

	segmentFiles, err := wal.getSegmentsFilesFromRange(0, 0)
	assert.NoError(t, err)

	expected := []string{filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, 0))}
	assert.Equal(t, expected, segmentFiles, "The segment files should match the expected segment files.")
}

func TestReadEntriesFromFile(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data"), CRC: 1}
	entries := []*xwalpb.WALEntry{entry}

	err := wal.Write(entries)
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

	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data"), CRC: 1}
	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("test data"), CRC: 1}
	entries := []*xwalpb.WALEntry{entry1, entry2}

	err := wal.Write(entries)
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

func TestGetLastSequencyNumberWithNoCurrentSegmentFile(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()
	wal.currentSegmentFile = nil

	err := wal.getLastLogSequencyNumber()
	assert.Error(t, err)
}

func TestCleanLogs(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()
	wal.cfg.CleanLogsInterval = 1 * time.Millisecond

	// create a fake file with .garbage extension
	// to be cleaned by calling directly the cleanGarbageLogs method
	garbageFile, err := os.Create(filepath.Join(wal.cfg.DirPath, "garbage.garbage"))
	assert.NoError(t, err)

	err = wal.deleteStaleFiles()
	assert.NoError(t, err)

	_, err = os.Stat(garbageFile.Name())
	assert.Error(t, err)
}

func TestRotateSegmentFile(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()

	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data"), CRC: 1}
	entries := []*xwalpb.WALEntry{entry}

	err := wal.Write(entries)
	assert.NoError(t, err)

	err = wal.rotateSegmentsFile()
	assert.NoError(t, err)

	// Verify the segment file was rotated correctly.
	_, err = os.Stat(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, 1)))
	assert.NoError(t, err)
}

func TestExtractSegmentIndexShouldReturnError(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()

	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data"), CRC: 1}
	entries := []*xwalpb.WALEntry{entry}

	err := wal.Write(entries)
	assert.NoError(t, err)

	segmentFile := filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, 0))
	newSegmentFile := segmentFile + LFSGarbageFileExtension
	err = os.Rename(segmentFile, newSegmentFile)
	assert.NoError(t, err)

	_, err = wal.extractSegmentIndex(newSegmentFile)
	assert.ErrorIs(t, err, ErrInvalidSegmentIndex)
}

func TestFirstSegmentIndexIsThefirstFileInTheSequence(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		panic(err)
	}

	cfg := LocalFSConfig{
		DirPath: dir,
	}

	wal := NewLocalFSWALBackend(&cfg)

	// create 3 files in the directory using os.Create
	for i := 50; i < 53; i++ {
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, i)))
		assert.NoError(t, err)
		file.Close()
	}

	err = wal.extractSegmentsIndexesFromFiles()
	assert.NoError(t, err)

	assert.Equal(t, uint32(50), wal.firstSegmentIndex)
	assert.Equal(t, uint32(52), wal.lastSegmentIndex)
}

func TestFirstSegmentIndexSkipsGarbageFiles(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		panic(err)
	}

	cfg := LocalFSConfig{
		DirPath: dir,
	}

	wal := NewLocalFSWALBackend(&cfg)

	// create 2 garbage files in the directory using os.Create
	for i := 48; i < 50; i++ {
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, i)+LFSGarbageFileExtension))
		assert.NoError(t, err)
		file.Close()
	}

	// create 3 files in the directory using os.Create
	for i := 50; i < 53; i++ {
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, i)))
		assert.NoError(t, err)
		file.Close()
	}

	err = wal.extractSegmentsIndexesFromFiles()
	assert.NoError(t, err)

	assert.Equal(t, uint32(50), wal.firstSegmentIndex)
	assert.Equal(t, uint32(52), wal.lastSegmentIndex)
}

func TestGetDirectorySize(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		panic(err)
	}

	cfg := LocalFSConfig{
		DirPath:            dir,
		SegmentsFileSizeMB: 1000,
	}

	wal := NewLocalFSWALBackend(&cfg)

	// create 3 files in the directory using os.Create
	for i := 50; i < 53; i++ {
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, i)))
		assert.NoError(t, err)
		file.Close()
	}
	wal.firstSegmentIndex = 50
	wal.lastSegmentIndex = 52

	size := wal.getDirectorySize()
	// 3 files with 1000 Mb are equal to 3000 Mb. Divided by 1024 to get the size in GB is 2.9296875.
	assert.Greater(t, size, float32(2.9))
}

func TestRotateSegmentsFileWhenDirectorySizeHasReachedTheLimit(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()
	wal.cfg.SegmentsFileSizeMB = 1000
	wal.cfg.SegmentsDirSizeGB = 3

	for i := 1; i < 5; i++ { // creating 4 fake files
		file, err := os.Create(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, i)))
		assert.NoError(t, err)
		file.Close()
	}
	wal.lastSegmentIndex = 5 // set the last segment index to 5 which is the index of the last created file above

	// Since getDirectorySize returns 4.0 GB and the limit is 3.0 GB, the first segment file should be set to garbage
	err := wal.rotateSegmentsFileIfNeeded()
	assert.NoError(t, err)

	// Verify the first segment file was set to garbage
	_, err = os.Stat(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, wal.firstSegmentIndex)+LFSGarbageFileExtension))
	assert.NoError(t, err)
}

func TestSetSegmentFileAsGarbage(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()

	fakeFileIndex := 50
	toBeGarbageFile, err := os.Create(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, fakeFileIndex)))
	assert.NoError(t, err)
	toBeGarbageFile.Close()

	err = wal.setSegmentFileAsGarbage(toBeGarbageFile.Name())
	assert.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, fakeFileIndex)+LFSGarbageFileExtension))
	assert.NoError(t, err)
}

func TestCreateCheckpoint(t *testing.T) {
	wal, dir := setupLocalFSWALBackend()

	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data"), CRC: 1}
	entries := []*xwalpb.WALEntry{entry}

	err := wal.Write(entries)
	assert.NoError(t, err)

	checkpointIndex, err := wal.CreateCheckpoint()
	assert.NoError(t, err)

	// Verify the checkpoint was created correctly.
	_, err = os.Stat(filepath.Join(dir, fmt.Sprintf(LFSWALSegmentFileFormat, checkpointIndex)+LFSCheckpointFileExtension))
	assert.NoError(t, err)
}

func TestReplayFromCheckpoint(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data 1")}
	entry1.CRC, _ = entry1.Checksum()

	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("test data 2")}
	entry2.CRC, _ = entry2.Checksum()

	entries := []*xwalpb.WALEntry{entry1, entry2}

	err := wal.Write(entries)
	assert.NoError(t, err)

	checkpointIndex, err := wal.CreateCheckpoint()
	assert.NoError(t, err)

	channel := make(chan *xwalpb.WALEntry, 2)
	err = wal.ReplayFromCheckpoint(channel, checkpointIndex, false)
	assert.NoError(t, err)
	close(channel)

	replayedEntries := make([]*xwalpb.WALEntry, 0)
	for entry := range channel {
		replayedEntries = append(replayedEntries, entry)
	}

	// Verify the replayed entries are correct.
	assert.Equal(t, len(entries), len(replayedEntries), "The replayed entries length should match the written entries length.")
}

func TestReplayToCheckpoint(t *testing.T) {
	wal, _ := setupLocalFSWALBackend()

	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("test data 1")}
	entry1.CRC, _ = entry1.Checksum()

	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("test data 2")}
	entry2.CRC, _ = entry2.Checksum()

	entries := []*xwalpb.WALEntry{entry1, entry2}

	err := wal.Write(entries)
	assert.NoError(t, err)

	checkpointIndex, err := wal.CreateCheckpoint()
	assert.NoError(t, err)

	channel := make(chan *xwalpb.WALEntry, 2)
	err = wal.ReplayToCheckpoint(channel, checkpointIndex, false)
	assert.NoError(t, err)
	close(channel)

	replayedEntries := make([]*xwalpb.WALEntry, 0)
	for entry := range channel {
		replayedEntries = append(replayedEntries, entry)
	}

	// Verify the replayed entries are correct.
	assert.Equal(t, len(entries), len(replayedEntries), "The replayed entries length should match the written entries length.")
}
