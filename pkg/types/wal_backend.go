package types

type WALBackendInterface interface {
	// Write appends a new entry to the log.
	Write(entry LogEntry) error

	// Read returns a log entry at a specific index.
	// If the entry does not exist, an error should be returned.
	Read(index int64) (LogEntry, error)

	// Replays log from beginning
	Replay() error

	// Flush ensures that all buffered log entries are written to the storage.
	Flush() error

	// Close performs any necessary cleanup operations to safely terminate the WAL.
	Close() error

	// LastIndex returns the index of the last entry written to the log.
	// This can be used to resume operations after a restart.
	LastIndex() (int64, error)
}
