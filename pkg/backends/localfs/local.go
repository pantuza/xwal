package localfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"google.golang.org/protobuf/encoding/protodelim"
)

const (
	// Default directory permission for the Local File System Backend
	LFSDefaultDirPermission = 0744

	// Segments files of the WAL are named using this format
	LFSWALSegmentFileFormat = "wal_%05d"

	// Files with this extension are considered garbage and will be deleted
	LFSGarbageFileExtension = ".garbage"
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
	lastLSN            uint64
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

	if err := wal.getLastLogSequencyNumber(); err != nil {
		return err
	}

	return nil
}

func (wal *LocalFSWALBackend) Type() types.WALBackendType {
	return types.LocalFileSystemWALBackend
}

func (wal *LocalFSWALBackend) createWALDir() error {
	if err := os.MkdirAll(wal.cfg.DirPath, LFSDefaultDirPermission); err != nil {
		return fmt.Errorf("Error Opening WAL. Check if the directory exists and has the right permissions. Error: %s", err)
	}
	return nil
}

func (wal *LocalFSWALBackend) extractSegmentsIndexesFromFiles() error {
	files, err := os.ReadDir(wal.cfg.DirPath)
	if err != nil {
		return fmt.Errorf("Error Opening WAL. Could not read directory entries. Error: %s", err)
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
			return fmt.Errorf("Error creating the current Segment File '%s'. Error: %s", filename, err)
		}
		wal.currentSegmentFile = file

	} else { // We open the last segment file
		file, err := os.OpenFile(filepath.Join(wal.cfg.DirPath, filename), os.O_APPEND|os.O_WRONLY, LFSDefaultDirPermission)
		if err != nil {
			return fmt.Errorf("Error opening the current Segment File '%s'. Error: %s", filename, err)
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
		if _, err := protodelim.MarshalTo(&buffer, entry); err != nil {
			return fmt.Errorf("Error marshaling entry to segment buffer before writing to file. Error: %s", err)
		}
	}

	if _, err := buffer.WriteTo(wal.currentSegmentFile); err != nil {
		return fmt.Errorf("Error writing all entries to segment file. Error: %s", err)
	}

	if err := wal.currentSegmentFile.Sync(); err != nil { // Flushes file to disk
		return fmt.Errorf("Error syncing segment file. Error: %s", err)
	}

	return nil
}

func (wal *LocalFSWALBackend) Read(index int64) (xwalpb.WALEntry, error) {
	return xwalpb.WALEntry{}, nil
}

func (wal *LocalFSWALBackend) Replay(channel chan *xwalpb.WALEntry) error {
	segmentsFiles, err := wal.getSegmentsFilesFromRange(wal.firstSegmentIndex, wal.lastSegmentIndex)
	if err != nil {
		return fmt.Errorf("Error getting segment files from range. Error: %s", err)
	}

	for _, segmentFile := range segmentsFiles {

		file, err := os.Open(segmentFile)
		if err != nil {
			return fmt.Errorf("Error opening segment file '%s' for replay. Error: %s", segmentFile, err)
		}
		readedEntries, err := wal.readEntriesFromFile(file)
		if err != nil {
			return fmt.Errorf("Error reading entries from segment file '%s' for replay. Error: %s", segmentFile, err)
		}
		file.Close()

		for _, entry := range readedEntries {
			if entry != nil {
				channel <- entry
			}
		}
	}

	return nil
}

func (wal *LocalFSWALBackend) getSegmentsFilesFromRange(start, end uint32) ([]string, error) {
	var files []string
	for i := start; i <= end; i++ {
		files = append(files, filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, i)))
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("No segment files found in the range from %d to %d", start, end)
	}

	return files, nil
}

// Reads entries from a file into a slice of WALEntry.
func (wal *LocalFSWALBackend) readEntriesFromFile(file *os.File) ([]*xwalpb.WALEntry, error) {
	var entries []*xwalpb.WALEntry

	fileData, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("Error reading segment file. Error: %s", err)
	}

	reader := bytes.NewReader(fileData)

	for reader.Len() > 0 {

		entry := &xwalpb.WALEntry{}

		if err := protodelim.UnmarshalFrom(reader, entry); err != nil {
			return nil, fmt.Errorf("Error unmarshaling entry from segment file. Error: %s", err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (wal *LocalFSWALBackend) getLastLogSequencyNumber() error {
	if wal.currentSegmentFile == nil {
		return fmt.Errorf("No current segment file to get the last LSN. Try opening the wal first. wal.Open() method")
	}

	file, err := os.Open(wal.currentSegmentFile.Name())
	if err != nil {
		return fmt.Errorf("Error opening current segment file for getting the last log sequency number. Error: %s", err)
	}

	entries, err := wal.readEntriesFromFile(file)
	if err != nil {
		return fmt.Errorf("Error reading entries from current segment file. Error: %s", err)
	}

	wal.lastLSN = 0
	if len(entries) > 0 {
		wal.lastLSN = entries[len(entries)-1].GetLSN()
	}

	return nil
}

func (wal *LocalFSWALBackend) Flush() error {
	return nil
}

func (wal *LocalFSWALBackend) Close() error {
	return nil
}

func (wal *LocalFSWALBackend) LastIndex() uint64 {
	return wal.lastLSN
}

func (wal *LocalFSWALBackend) IncLastIndex() {
	wal.lastLSN++
}
