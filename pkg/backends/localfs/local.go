package localfs

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"google.golang.org/protobuf/encoding/protodelim"
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
	var entries []*xwalpb.WALEntry

	segmentsFiles, err := wal.getSegmentsFilesFromRange(wal.firstSegmentIndex, wal.lastSegmentIndex)
	if err != nil {
		return nil, fmt.Errorf("Error getting segment files from range. Error: %s", err)
	}

	for _, segmentFile := range segmentsFiles {

		file, err := os.Open(segmentFile)
		if err != nil {
			return nil, fmt.Errorf("Error opening segment file '%s' for replay. Error: %s", segmentFile, err)
		}
		readedEntries, err := wal.readEntriesFromFile(file)
		if err != nil {
			return nil, fmt.Errorf("Error reading entries from segment file '%s' for replay. Error: %s", segmentFile, err)
		}
		file.Close()

		// Append the current readed entries to the slice of entries
		entries = append(entries, readedEntries...)
	}

	return entries, nil
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

	fileData, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error reading segment file. Error: %s", err))
	}

	for len(fileData) > 0 {
		// Decode the length of the next entry (varint).
		len64, n := proto.DecodeVarint(fileData)
		if n <= 0 {
			return nil, errors.New(fmt.Sprintf("Error reading entry length from segment file. Error: %s", err))
		}
		fileData = fileData[n:] // Advance past the varint

		if len(fileData) < int(len64) {
			return nil, errors.New(fmt.Sprintf("Error reading entry data from segment file. Error: remaining file size should not be smaller than entry length: %d < %d", len(fileData), int(len64)))
		}

		// Deserialize the entry.
		entryData := fileData[:len64]
		entry := &xwalpb.WALEntry{}
		if err := proto.Unmarshal(entryData, entry); err != nil {
			return nil, errors.New(fmt.Sprintf("Error unmarshaling entry from segment file. Error: %s", err))
		}

		entries = append(entries, entry)

		// Advance the slice past this entry's data for the next iteration.
		fileData = fileData[len64:]
	}

	return entries, nil
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
