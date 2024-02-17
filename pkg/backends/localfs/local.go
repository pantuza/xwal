package localfs

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

const (
	LFSDefaultDirPermission = 0644

	LFSWALSegmentFileFormat = "wal_%05d"
)

type LocalFSConfig struct {

	// Name of the directory where WAL files will be stored
	DirPath string `yaml:"dirPath"`

	// Size in megabytes of each segment inside files
	SegmentsSize int `yaml:"segmentsSize"`

	// Size in megabytes of each file inside the WAL
	FileSize int `yaml:"fileSize"`
}

type LocalFSWALBackend struct {
	cfg *LocalFSConfig

	firstSegmentIndex uint32
	lastSegmentIndex  uint32

	currentSegmentFile *os.File
}

func NewLocalFSWALBackend(cfg *LocalFSConfig) *LocalFSWALBackend {
	return &LocalFSWALBackend{cfg: cfg, firstSegmentIndex: 0, lastSegmentIndex: 0}
}

func (wal *LocalFSWALBackend) Open() error {
	if err := wal.createWALDir(); err != nil {
		return err
	}
	if err := wal.extractSegmentsIndexesFromFiles(); err != nil {
		return err
	}
	if err := wal.openCurrentSegmentFile(); err != nil {
		return err
	}

	return nil
}

func (wal *LocalFSWALBackend) Type() types.WALBackendType {
	return types.LocalFileSystemWALBackend
}

func (wal *LocalFSWALBackend) createWALDir() error {
	if err := os.MkdirAll(wal.cfg.DirPath, LFSDefaultDirPermission); err != nil {
		return errors.New(fmt.Sprintf("Error Opening WAL. Check if the directory exists and has the right permissions. Error: %s", err))
	}
	return nil
}

func (wal *LocalFSWALBackend) extractSegmentsIndexesFromFiles() error {
	files, err := os.ReadDir(wal.cfg.DirPath)
	if err != nil {
		return errors.New(fmt.Sprintf("Error Opening WAL. Could not read directory entries. Error: %s", err))
	}

	for _, file := range files {
		index := wal.extractSegmentIndex(file.Name())
		if index > wal.lastSegmentIndex {
			wal.lastSegmentIndex = index
		}
		if index < wal.firstSegmentIndex {
			wal.firstSegmentIndex = index
		}
	}
	return nil
}

func (wal *LocalFSWALBackend) openCurrentSegmentFile() error {
	filename := fmt.Sprintf(LFSWALSegmentFileFormat, wal.lastSegmentIndex)

	if wal.lastSegmentIndex == 0 { // Then, we create the first segment file

		file, err := os.Create(filepath.Join(wal.cfg.DirPath, filename))
		if err != nil {
			return errors.New(fmt.Sprintf("Error creating the current Segment File '%s'. Error: %s", filename, err))
		}
		wal.currentSegmentFile = file

	} else { // We open the last segment file
		file, err := os.OpenFile(filepath.Join(wal.cfg.DirPath, filename), os.O_APPEND|os.O_WRONLY, LFSDefaultDirPermission)
		if err != nil {
			return errors.New(fmt.Sprintf("Error opening the current Segment File '%s'. Error: %s", filename, err))
		}
		wal.currentSegmentFile = file
	}

	return nil
}

// Segments files names format is: wal_00000. The last 5 characters are the index.
// This function extracts the segment Index.
func (wal *LocalFSWALBackend) extractSegmentIndex(filename string) uint32 {
	var index uint32
	fmt.Sscanf(filename, LFSWALSegmentFileFormat, &index)
	return index
}

// Write writes the entries to the current segment file.
func (wal *LocalFSWALBackend) Write(entries []*xwalpb.WALEntry) error {

	// Initialize a bytes buffer to accumulate the serialized data
	var buffer bytes.Buffer

	for _, entry := range entries {

		data, err := proto.Marshal(entry)
		if err != nil {
			return errors.New(fmt.Sprintf("Error Marshaling entries before writing to segment file. Error: %s", err))
		}

		// Encode the length of the entry data as a varint and write it to the buffer
		lenBuf := proto.EncodeVarint(uint64(len(data)))
		if _, err := buffer.Write(lenBuf); err != nil {
			return errors.New(fmt.Sprintf("Error encoding entry length before writing to segment file. Error: %s", err))
		}

		// Write the entry data to the buffer
		if _, err := buffer.Write(data); err != nil {
			return errors.New(fmt.Sprintf("Error writing serialized entry as bytes before writing to segment file. Error: %s", err))
		}
	}

	if _, err := buffer.WriteTo(wal.currentSegmentFile); err != nil {
		return errors.New(fmt.Sprintf("Error writing all entries to segment file. Error: %s", err))
	}

	return nil
}

func (wal *LocalFSWALBackend) Read(index int64) (xwalpb.WALEntry, error) {
	return xwalpb.WALEntry{}, nil
}

func (wal *LocalFSWALBackend) Replay() ([]*xwalpb.WALEntry, error) {
	return nil, nil
}

func (wal *LocalFSWALBackend) Flush() error {
	return nil
}

func (wal *LocalFSWALBackend) Close() error {
	return nil
}

func (wal *LocalFSWALBackend) LastIndex() uint32 {
	return wal.lastSegmentIndex
}
