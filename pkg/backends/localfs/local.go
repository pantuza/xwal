package localfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

const (
	// Default directory permission for the Local File System Backend
	LFSDefaultDirPermission = 0744

	// Segments files of the WAL are named using this format
	LFSWALSegmentFileFormat = "wal_%05d"

	// Files with this extension are considered garbage and will be deleted
	LFSGarbageFileExtension = ".garbage"

	// Files with this extension are considered checkpoints
	LFSCheckpointFileExtension = ".checkpoint"
)

// Error message for when trying to extract an invalid segment index from a .garbage file
var ErrInvalidSegmentIndex = errors.New("Invalid segment index. The file is a .garbage file")

type LocalFSWALBackend struct {
	cfg *LocalFSConfig

	ctx               context.Context
	cancel            context.CancelFunc
	waitGroup         sync.WaitGroup
	cleanLogsInterval *time.Ticker

	firstSegmentIndex uint32
	lastSegmentIndex  uint32

	currentSegmentFile *os.File
	lastLSN            uint64
	logger             *zap.Logger
}

func NewLocalFSWALBackend(cfg *LocalFSConfig) *LocalFSWALBackend {
	if cfg.CleanLogsInterval == 0 {
		cfg.CleanLogsInterval = 1 * time.Minute // Default interval to clean garbage logs
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &LocalFSWALBackend{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		waitGroup:         sync.WaitGroup{},
		cleanLogsInterval: time.NewTicker(cfg.CleanLogsInterval),
		firstSegmentIndex: 0,
		lastSegmentIndex:  0,
		lastLSN:           0,
		logger:            cfg.Logger,
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

	wal.waitGroup.Add(1)
	go wal.cleanGarbageLogs()

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

	wal.firstSegmentIndex = math.MaxUint32
	wal.lastSegmentIndex = 0

	for _, file := range files {

		index, err := wal.extractSegmentIndex(file.Name())
		if err == ErrInvalidSegmentIndex {
			// Skip garbage files but update the first segment file with the next index right after the garbage file
			wal.firstSegmentIndex = index + 1
			continue
		}

		if index > wal.lastSegmentIndex {
			wal.lastSegmentIndex = index
		}
		if index < wal.firstSegmentIndex {
			wal.firstSegmentIndex = index
		}
	}

	if wal.firstSegmentIndex == math.MaxUint32 {
		// If firstSegmentIndex keeps being the maximum value,
		// it means there are no segment files and we should start from 0
		wal.firstSegmentIndex = 0
	}

	return nil
}

func (wal *LocalFSWALBackend) openCurrentSegmentFile() error {
	filename := fmt.Sprintf(LFSWALSegmentFileFormat, wal.lastSegmentIndex)

	// If file does not exist, it will be created. Otherwise, it will be opened.
	file, err := os.OpenFile(filepath.Join(wal.cfg.DirPath, filename), os.O_APPEND|os.O_WRONLY|os.O_CREATE, LFSDefaultDirPermission)
	if err != nil {
		return fmt.Errorf("Error opening the current Segment File '%s'. Error: %s", filename, err)
	}
	wal.currentSegmentFile = file

	return nil
}

// Segments files names format is: wal_00000. The last 5 characters are the index.
// This function extracts the segment Index.
func (wal *LocalFSWALBackend) extractSegmentIndex(filename string) (uint32, error) {
	var index uint32
	if _, err := fmt.Sscanf(filename, LFSWALSegmentFileFormat, &index); err != nil {
		return 0, fmt.Errorf("Error extracting segment index from file name. Error: %s", err)
	}

	// Garbage files should be skipped and not considered for the segment index
	if filepath.Ext(filename) == LFSGarbageFileExtension {
		return index, ErrInvalidSegmentIndex
	}

	return index, nil
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

	if err := wal.rotateSegmentsFileIfNeeded(); err != nil {
		return err
	}

	if _, err := buffer.WriteTo(wal.currentSegmentFile); err != nil {
		return fmt.Errorf("Error writing all entries to segment file. Error: %s", err)
	}

	if err := wal.currentSegmentFile.Sync(); err != nil { // Flushes file to disk
		return fmt.Errorf("Error syncing segment file. Error: %s", err)
	}

	return nil
}

// rotateSegmentsFileIfNeeded checks if the current segment file reached the maximum size.
// If so, call the rotate method
func (wal *LocalFSWALBackend) rotateSegmentsFileIfNeeded() error {
	fileInfo, _ := wal.currentSegmentFile.Stat()

	// Rotates the current segment file if it reaches the maximum size
	if fileInfo.Size() >= int64(wal.cfg.SegmentsFileSizeMB)*1024*1024 {
		if err := wal.rotateSegmentsFile(); err != nil {
			return err
		}
	}

	// Rotates (setting it as garbage) the first segment file if the directory size reaches ruffly the maximum size
	if wal.getDirectorySize() > float32(wal.cfg.SegmentsDirSizeGB) {
		firstSegmentFileName := filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, wal.firstSegmentIndex))
		if err := wal.setSegmentFileAsGarbage(firstSegmentFileName); err != nil {
			return err
		}
	}

	return nil
}

// Ruffly calculates the directory size by multiplying the number of files by the segment file size.
// It does not consider the actual size of the files. In Golang in order to get the actual size of a directory
// we would need to walk through all files and sum their sizes. This is a very expensive operation. It would be
// called on every Write operation. Thus, we are using this ruffly calculation.
func (wal *LocalFSWALBackend) getDirectorySize() float32 {
	numberOfFiles := uint32(wal.lastSegmentIndex-wal.firstSegmentIndex) + 1
	return float32(uint32(wal.cfg.SegmentsFileSizeMB)*numberOfFiles) / 1024 // in Gb
}

// Rotates current segments file. It closes the actual segment file and opens a new one.
// It also increments the last segment index.
func (wal *LocalFSWALBackend) rotateSegmentsFile() error {
	wal.currentSegmentFile.Close()
	wal.lastSegmentIndex++

	if err := wal.openCurrentSegmentFile(); err != nil {
		return fmt.Errorf("Error trying to rotate current segment file (%d). Error: %s", wal.lastSegmentIndex, err)
	}

	return nil
}

// CreateCheckpoint creates a checkpoint on the WAL. It closes the current segment file and renames it to a checkpoint file.
// It also opens a new current segment file. It returns an error if it could not open the new current segment file.
// It returns the checkpoint index. The caller should store it for later replay from that particular checkpoint.
func (wal *LocalFSWALBackend) CreateCheckpoint() (uint64, error) {
	wal.currentSegmentFile.Close()
	checkpointIndex := wal.lastSegmentIndex
	wal.lastSegmentIndex++

	checkpointFileName := filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, checkpointIndex)+LFSCheckpointFileExtension)
	if err := os.Rename(wal.currentSegmentFile.Name(), checkpointFileName); err != nil {
		return 0, fmt.Errorf("Error renaming current segment file to a checkpoint file. Error: %s", err)
	}

	if err := wal.openCurrentSegmentFile(); err != nil {
		return 0, fmt.Errorf("Error trying to open current segment file (%d) right after creating a checkpoint. Error: %s", checkpointIndex, err)
	}

	return uint64(checkpointIndex), nil
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

// Replays WAL from the given checkpoint til the end of the WAL. It can be
// replayed backwards: from the end of the WAL til the given checkpoint.
// Entries read from the WAL should be sent to the given channel.
func (wal *LocalFSWALBackend) ReplayFromCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error {
	if checkpoint > uint64(wal.lastSegmentIndex) {
		return fmt.Errorf("Invalid checkpoint provided. Checkpoint: %d, Last Segment file Index: %d", checkpoint, wal.lastSegmentIndex)
	}

	segmentsFiles, err := wal.getSegmentsFilesFromOrToCheckpoint(uint32(checkpoint), wal.lastSegmentIndex, uint32(checkpoint))
	if err != nil {
		return fmt.Errorf("Error getting segment files from range while replaying from checkpoint. Error: %s", err)
	}

	if backwards {
		slices.Reverse(segmentsFiles)
	}

	return wal.replaySegments(segmentsFiles, channel, backwards)
}

// Replays WAL from the beginning of the WAL til the given checkpoint. It can be
// replayed backwards: from the given checkpoint til the beginning of the WAL.
// Entries read from the WAL should be sent to the given channel.
func (wal *LocalFSWALBackend) ReplayToCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error {
	if checkpoint > uint64(wal.lastSegmentIndex) {
		return fmt.Errorf("Invalid checkpoint provided. Checkpoint: %d, Last Segment file Index: %d", checkpoint, wal.lastSegmentIndex)
	}

	segmentsFiles, err := wal.getSegmentsFilesFromOrToCheckpoint(wal.firstSegmentIndex, uint32(checkpoint), uint32(checkpoint))
	if err != nil {
		return fmt.Errorf("Error getting segment files from range while replaying to checkpoint. Error: %s", err)
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

// getSegmentsFilesFromOrToCheckpoint returns the segment files from the given range considering the checkpoint.
// Checkpoint files have a suffix that should be considered when opening the file.
func (wal *LocalFSWALBackend) getSegmentsFilesFromOrToCheckpoint(start, end, checkpoint uint32) ([]string, error) {
	var files []string
	suffix := ""
	for i := start; i <= end; i++ {

		// If the current segment file is the checkpoint, we should add the checkpoint suffix to the file name otherwise we won't be able to open it.
		if i == checkpoint {
			suffix = LFSCheckpointFileExtension
		}
		files = append(files, filepath.Join(wal.cfg.DirPath, fmt.Sprintf(LFSWALSegmentFileFormat, i))+suffix)
		suffix = ""
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
					wal.logger.Error("Fail to validate entry checksum. Skiping replaying entry", zap.String("segmentFile", segmentFile), zap.Uint64("LSN", entry.LSN))
				}

				if entry.CRC != chksum {
					wal.logger.Warn("Entry Checksum does not match! Skiping replaying entry", zap.String("segmentFile", segmentFile), zap.Uint64("LSN", entry.LSN), zap.Uint32("EntryCRC", entry.CRC), zap.Uint32("CalculatedCRC", chksum))
					continue
				}
				channel <- entry
			}
		}

		// Rename the file to a garbage file that will be deleted asynchronously
		if err := wal.setSegmentFileAsGarbage(segmentFile); err != nil {
			return err
		}
	}

	// Once we have replayed everything, we should rotate the current segment file.
	if err := wal.rotateSegmentsFile(); err != nil {
		return fmt.Errorf("Error rotating current segment file after replaying wal. Error: %s", err)
	}

	// update the first segment file to be the new current segment file so next time we replay we start from it
	wal.firstSegmentIndex = wal.lastSegmentIndex

	return nil
}

// setSegmentFileAsGarbage renames the given segment file to a garbage file that will be deleted asynchronously.
func (wal *LocalFSWALBackend) setSegmentFileAsGarbage(segmentFile string) error {
	if err := os.Rename(segmentFile, segmentFile+LFSGarbageFileExtension); err != nil {
		return fmt.Errorf("Error renaming segment file to garbage file. Error: %s", err)
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

func (wal *LocalFSWALBackend) cleanGarbageLogs() {
	defer wal.waitGroup.Done()

	for {
		select {
		case <-wal.cleanLogsInterval.C:

			if err := wal.deleteStaleFiles(); err != nil {
				wal.logger.Error("Error cleaning Garbage Logs from inside goroutine. Error", zap.Error(err))
			}

		case <-wal.ctx.Done():
			wal.logger.Info("Cleaning Garbage Logs before exiting")
			if err := wal.deleteStaleFiles(); err != nil {
				wal.logger.Error("Error cleaning Garbage Logs before exiting", zap.Error(err))
			}

			return // This ends this Goroutine
		}
	}
}

func (wal *LocalFSWALBackend) deleteStaleFiles() error {
	files, err := os.ReadDir(wal.cfg.DirPath)
	if err != nil {
		wal.logger.Error("Error reading directory entries for deletion", zap.Error(err))
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".garbage" {
			os.Remove(filepath.Join(wal.cfg.DirPath, file.Name()))
		}
	}

	return nil
}

func (wal *LocalFSWALBackend) Close() error {
	wal.logger.Info("Closing LocalFS WAL Backend")

	// closes current segment file
	if err := wal.currentSegmentFile.Close(); err != nil {
		return fmt.Errorf("LocalFSWALBackend shutdown: Error closing current segment file. Error: %s", err)
	}

	wal.cleanLogsInterval.Stop()
	wal.cancel()

	wal.waitGroup.Wait()
	return nil
}

func (wal *LocalFSWALBackend) LastIndex() uint64 {
	return wal.lastLSN
}

func (wal *LocalFSWALBackend) IncLastIndex() {
	wal.lastLSN++
}
