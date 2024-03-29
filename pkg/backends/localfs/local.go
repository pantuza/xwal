package localfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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

	// Interval to clean garbage logs
	CleanLogsInterval time.Duration `yaml:"cleanLogsInterval"`
}

type LocalFSWALBackend struct {
	cfg *LocalFSConfig

	ctx               context.Context
	cleanLogsInterval *time.Ticker

	firstSegmentIndex uint32
	lastSegmentIndex  uint32

	currentSegmentFile *os.File
	lastLSN            uint64
}

func NewLocalFSWALBackend(cfg *LocalFSConfig) *LocalFSWALBackend {
	if cfg.CleanLogsInterval == 0 {
		cfg.CleanLogsInterval = 1 * time.Minute // Default interval to clean garbage logs
	}

	return &LocalFSWALBackend{
		cfg:               cfg,
		ctx:               context.Background(),
		cleanLogsInterval: time.NewTicker(cfg.CleanLogsInterval),
		firstSegmentIndex: 0,
		lastSegmentIndex:  0,
		lastLSN:           0,
	}
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

	go wal.cleanGarbageLogs(wal.ctx)

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

// Replay replays the entries from the segment files to the channel.
// It allows to replay the entries in a backwards order.
// It always consider the entire WAL. Thus, from the first segment file to the last segment file or backwards.
func (wal *LocalFSWALBackend) Replay(channel chan *xwalpb.WALEntry, backwards bool) error {
	segmentsFiles, err := wal.getSegmentsFilesFromRange(wal.firstSegmentIndex, wal.lastSegmentIndex)
	if err != nil {
		return fmt.Errorf("Error getting segment files from range. Error: %s", err)
	}

	if backwards {
		slices.Reverse(segmentsFiles)
	}

	return wal.replaySegments(segmentsFiles, channel, backwards)
}

// ReplayFromRange replays the entries from the segment files to the channel.
// It allows to replay the entries from the given range of Segments Files in a backwards order.
// It always respects the range provided. If a invalid range is provided, it will return an error.
func (wal *LocalFSWALBackend) ReplayFromRange(channel chan *xwalpb.WALEntry, backwards bool, start, end uint32) error {
	if start < wal.firstSegmentIndex || end > wal.lastSegmentIndex {
		return fmt.Errorf("Invalid range provided. Start: %d, End: %d. First Segment file Index: %d, Last Segment file Index: %d", start, end, wal.firstSegmentIndex, wal.lastSegmentIndex)
	}

	segmentsFiles, err := wal.getSegmentsFilesFromRange(start, end)
	if err != nil {
		return fmt.Errorf("Error getting segment files from range. Error: %s", err)
	}

	if backwards {
		slices.Reverse(segmentsFiles)
	}

	return wal.replaySegments(segmentsFiles, channel, backwards)
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

// replaySegments replays the entries from the segment files to the channel.
// It also renames the segment files to a garbage file that will be deleted asynchronously.
//
// This method do care about Ordering. Given the list of segments files, it will replay them in the order they are provided.
func (wal *LocalFSWALBackend) replaySegments(segmentsFiles []string, channel chan *xwalpb.WALEntry, backwards bool) error {
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

		if backwards {
			slices.Reverse(readedEntries)
		}

		for _, entry := range readedEntries {
			if entry != nil {

				chksum, err := entry.Checksum()
				if err != nil {
					fmt.Printf("Fail to validate entry checksum. Skiping replaying entry: LSN=%d, SegmentFile=%s", entry.LSN, segmentFile)
				}

				if entry.CRC != chksum {
					fmt.Printf("Entry Checksum does not match! Skiping replaying entry: LSN=%d, SegmentFile=%s, EntryCRC: %d, CalculatedCRC: %d", entry.LSN, segmentFile, entry.CRC, chksum)
					continue
				}
				channel <- entry
			}
		}

		// Rename the file to a garbage file that will be deleted asynchronously
		if err := os.Rename(segmentFile, segmentFile+LFSGarbageFileExtension); err != nil {
			return fmt.Errorf("Error renaming segment file to garbage file. Error: %s", err)
		}
	}

	return nil
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

func (wal *LocalFSWALBackend) cleanGarbageLogs(ctx context.Context) {
	for {
		select {
		case <-wal.cleanLogsInterval.C:

			if err := wal.deleteStaleFiles(); err != nil {
				fmt.Printf("Error cleaning Garbage Logs from inside goroutine. Error: %s", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (wal *LocalFSWALBackend) deleteStaleFiles() error {
	files, err := os.ReadDir(wal.cfg.DirPath)
	if err != nil {
		fmt.Printf("Error reading directory entries for deletion. Error: %s", err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".garbage" {
			os.Remove(filepath.Join(wal.cfg.DirPath, file.Name()))
		}
	}

	return nil
}

func (wal *LocalFSWALBackend) Close() error {
	// closes current segment file
	if err := wal.currentSegmentFile.Close(); err != nil {
		return fmt.Errorf("LocalFSWALBackend shutdown: Error closing current segment file. Error: %s", err)
	}

	// stops the clean logs interval
	wal.cleanLogsInterval.Stop()
	wal.ctx.Done()

	return nil
}

func (wal *LocalFSWALBackend) LastIndex() uint64 {
	return wal.lastLSN
}

func (wal *LocalFSWALBackend) IncLastIndex() {
	wal.lastLSN++
}
