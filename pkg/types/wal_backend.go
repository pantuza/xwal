package types

import "github.com/pantuza/xwal/protobuf/xwalpb"

type WALBackendInterface interface {
	// Open initializes the WAL for reading and writing.
	//
	// This method must create or open the WAL and read
	// what is the last Segments storage file and the last LSN.
	//
	// This method should be called before any other operation because
	// it may perform necessary setup operations.
	Open() error

	// Write appends a list of entries to the log.
	// The entry should be written to the storage immediately.
	//
	// This method does not mean to write entries to in memory buffer.
	// It indeed is the last milestone before writing to the storage.
	//
	// It must return an error if the entries could not be written.
	Write(entries []*xwalpb.WALEntry) error

	// Replays log from beginning to end (FIFO), sending each entry to the provided channel.
	// If backwards is true, it should replay the log from end to beginning (LIFO).
	//
	// Things that Replay should make sure to care of:
	// - Verify if Checksum matches before sending an entry to replay channel
	// - Remove file (any entries storage) after reading all entries from it
	//
	// Be aware that end users are responsible to provide a function that
	// will handle entries sent to the channel. This method is only responsible
	// to fill up the channel and then exit.
	//
	// In case of any failure, it should return an error to the caller.
	Replay(channel chan *xwalpb.WALEntry, backwards bool) error

	// Replays log from a given range, sending each entry to the provided channel.
	// If backwards is true, it should replay the log from end to beginning (LIFO).
	// The range is inclusive, meaning that it should replay all entries from start to end.
	// If start is 0, it should replay from the beginning of the log.
	//
	// Start and end are the suffixes of the Segments files, not the LSN. They are non negative
	// values and start must be less than or equal to end.
	ReplayFromRange(channel chan *xwalpb.WALEntry, backwards bool, start, end uint32) error

	// Creates checkpoint on the WAL. Checkpoints are simply sufixes on the segments
	// files names that allow users to mark their WAL files by any domain specific
	// purpose. The method should close the current segments file and rename as the
	// new checkpoint. Checkpoints naming strategy follows the format:
	//     wal-00042.checkpoint
	// Thus, the higher the WAL Segments number, the more recent is the checkpoint.
	// The method returns the checkpoint segments file number so the user can store
	// it for later replay from that particular checkpoint further.
	CreateCheckpoint() (uint64, error)

	// Replays WAL from the given checkpoint til the end of the WAL. It can be
	// replayed backwards: from the end of the WAL til the given checkpoint.
	// Entries read from the WAL should be sent to the given channel.
	ReplayFromCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error

	// Replays WAL from the beginning of the WAL til the given checkpoint. It can be
	// replayed backwards: from the given checkpoint til the beginning of the WAL.
	// Entries read from the WAL should be sent to the given channel.
	ReplayToCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error

	// Close performs any necessary cleanup operations to safely terminate the WAL.
	// For example, it should close any open files and finish any pending writes.
	// Think of it as the graceful shutdown of the WAL.
	//
	// In case of any failure, it should return an error to the caller.
	Close() error

	// LastIndex returns the index of the last entry written to the WAL.
	// This can be used to resume operations after a restart.
	//
	// If no entry was written, it should return 0, meaning you are starting
	// a new WAL from scratch.
	LastIndex() uint64

	// Increments the index of the last entry written to the log.
	//
	// This method is useful while creating new WALEntry objects and
	// adding to the WAL.
	IncLastIndex()

	// Type returns the type of the WAL backend in a human readable format.
	// Useful to be used in a switch case to determine the backend type.
	Type() WALBackendType
}

// WALBackendType represents the type of the WAL backend.
type WALBackendType string

const (
	LocalFileSystemWALBackend WALBackendType = "local_filesystem"
	AWSS3WALBackend           WALBackendType = "aws_s3"
)
