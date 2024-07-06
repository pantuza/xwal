package awss3

import (
	"time"

	"go.uber.org/zap"
)

const (
	DefaultBucketName         = "xwal"
	DefaultSegmentsFileSizeMB = 1000
	DefaultSegmentsDirSizeGB  = 20
	DefaultCleanLogsInterval  = 1 * time.Minute
	DefaultProfile            = "default"
)

type AWSS3Config struct {
	// Name of the s3 bucket where WAL files will be stored
	BucketName string `yaml:"bucketName"`

	// AWS Region where the bucket is located
	Region string `yaml:"region"`

	// AWS Profile to be used (optional)
	Profile string `yaml:"profile,omitempty"`

	// AWS Auth credentials (optional). Default is to read from aws credentials file
	Auth struct {
		AccessKey string `yaml:"accessKey"`
		SecretKey string `yaml:"secretKey"`
	} `yaml:"auth,omitempty"`

	// Size in megabytes of each file inside the WAL
	SegmentsFileSizeMB int `yaml:"segmentsFileSize"`

	// Size in Gigabytes of the Segments Directory
	SegmentsDirSizeGB uint32 `yaml:"segmentsDirSizeGB"`

	// Interval to clean garbage logs
	CleanLogsInterval time.Duration `yaml:"cleanLogsInterval"`

	// Reference to zap logger. Does not get informed by yaml files. It is set by the xwal package
	Logger *zap.Logger
}

// DefaultAWSS3Config returns a default configuration for the AWSS3Config
func DefaultAWSS3Config() *AWSS3Config {
	return &AWSS3Config{
		BucketName:         DefaultBucketName,
		Profile:            DefaultProfile,
		SegmentsFileSizeMB: DefaultSegmentsFileSizeMB,
		SegmentsDirSizeGB:  DefaultSegmentsDirSizeGB,
		CleanLogsInterval:  DefaultCleanLogsInterval,
	}
}
