package localfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"google.golang.org/protobuf/proto"
)

const (
	LFSDefaultDirPermission = 0644

	LFSWALSegmentFileFormat = "wal_%05d"
)

type LocalFSConfig struct {
	// Name of the directory where WAL files will be stored
	DirPath string `yaml:"dirPath"`

	// Size in megabytes of each segment inside files
	SegmentsSize int `yaml:"segmentSize"`

	// Size in megabytes of each file inside the WAL
	FileSize int `yaml:"fileSize"`
}

type LocalFSBackend struct {
	cfg LocalFSConfig

	firstSegmentIndex uint32
	lastSegmentIndex  uint32

	currentSegmentFile *os.File
}

func NewLocalFSWALBackend(cfg LocalFSConfig) *LocalFSBackend {
	return &LocalFSBackend{cfg: cfg, firstSegmentIndex: 0, lastSegmentIndex: 0}
}

func (wal *LocalFSBackend) Open() error {
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

func (wal *LocalFSBackend) Type() types.WALBackendType {
	return types.LocalFileSystemWALBackend
}

func (wal *LocalFSBackend) createWALDir() error {
	if err := os.MkdirAll(wal.cfg.DirPath, LFSDefaultDirPermission); err != nil {
		return errors.New(fmt.Sprintf("Error Opening WAL. Check if the directory exists and has the right permissions. Error: %s", err))
	}
	return nil
}

func (wal *LocalFSBackend) extractSegmentsIndexesFromFiles() error {
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

func (wal *LocalFSBackend) openCurrentSegmentFile() error {
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
func (wal *LocalFSBackend) extractSegmentIndex(filename string) uint32 {
	var index uint32
	fmt.Sscanf(filename, LFSWALSegmentFileFormat, &index)
	return index
}

// Write writes the entries to the current segment file.
func (wal *LocalFSBackend) Write(entries []*xwalpb.WALEntry) error {
	var buffer []byte

	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return errors.New(fmt.Sprintf("Error Marshaling entries before writing to segment file. Error: %s", err))
		}

		buffer = append(buffer, data...)
	}

	if _, err := wal.currentSegmentFile.Write(buffer); err != nil {
		return errors.New(fmt.Sprintf("Error writing all entries to segment file. Error: %s", err))
	}

	return nil
}

func (wal *LocalFSBackend) Read(index int64) (xwalpb.WALEntry, error) {
	return xwalpb.WALEntry{}, nil
}

func (wal *LocalFSBackend) Replay() ([]*xwalpb.WALEntry, error) {
	return nil, nil
}

func (wal *LocalFSBackend) Flush() error {
	return nil
}

func (wal *LocalFSBackend) Close() error {
	return nil
}

func (wal *LocalFSBackend) LastIndex() uint32 {
	return wal.lastSegmentIndex
}
