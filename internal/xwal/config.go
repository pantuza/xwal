package xwal

import "time"

// The configuration of the xWAL. It will drive how the xWAL will use
// any WAL Backend implementation
type XWALConfig struct {

	// Name of the directory where WAL files will be stored
	DirName string

	// Number of segments allowed inside the in memory buffer
	BufferSize int32

	// Frequency that xWAL Flushes data from memory to target WAL Backend
	FlushFrequency time.Duration

	// Size in megabytes of each segment inside files
	SegmentSize int32

	// Size in megabytes of each file inside the WAL
	FileSize int32
}
