package localfs

import "time"

const (
	DefaultDirPath           = "/tmp/xwal"
	DefaultSegmentsFileSize  = 1000
	DefaultSegmentsDirSizeGB = 20
	DefaultCleanLogsInterval = 1 * time.Minute
)

type LocalFSConfig struct {
	// Name of the directory where WAL files will be stored
	DirPath string `yaml:"dirPath"`

	// Size in megabytes of each file inside the WAL
	SegmentsFileSize int `yaml:"segmentsFileSize"`

	// Size in Gigabytes of the Segments Directory
	SegmentsDirSizeGB uint32 `yaml:"segmentsDirSizeGB"`

	// Interval to clean garbage logs
	CleanLogsInterval time.Duration `yaml:"cleanLogsInterval"`
}

// DefaultLocalFSConfig returns a default configuration for the LocalFSConfig
func DefaultLocalFSConfig() *LocalFSConfig {
	return &LocalFSConfig{
		DirPath:           DefaultDirPath,
		SegmentsFileSize:  DefaultSegmentsFileSize,
		SegmentsDirSizeGB: DefaultSegmentsDirSizeGB,
		CleanLogsInterval: DefaultCleanLogsInterval,
	}
}
