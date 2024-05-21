package localfs

import (
	"time"

	"go.uber.org/zap"
)

const (
	DefaultDirPath            = "/tmp/xwal"
	DefaultSegmentsFileSizeMB = 1000
	DefaultSegmentsDirSizeGB  = 20
	DefaultCleanLogsInterval  = 1 * time.Minute
)

type LocalFSConfig struct {
	// Name of the directory where WAL files will be stored
	DirPath string `yaml:"dirPath"`

	// Size in megabytes of each file inside the WAL
	SegmentsFileSizeMB int `yaml:"segmentsFileSize"`

	// Size in Gigabytes of the Segments Directory
	SegmentsDirSizeGB uint32 `yaml:"segmentsDirSizeGB"`

	// Interval to clean garbage logs
	CleanLogsInterval time.Duration `yaml:"cleanLogsInterval"`

	// Reference to zap logger. Does not get informed by yaml files. It is set by the xwal package
	Logger *zap.Logger
}

// DefaultLocalFSConfig returns a default configuration for the LocalFSConfig
func DefaultLocalFSConfig() *LocalFSConfig {
	return &LocalFSConfig{
		DirPath:            DefaultDirPath,
		SegmentsFileSizeMB: DefaultSegmentsFileSizeMB,
		SegmentsDirSizeGB:  DefaultSegmentsDirSizeGB,
		CleanLogsInterval:  DefaultCleanLogsInterval,
	}
}
