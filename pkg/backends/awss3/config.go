package awss3

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"
)

const (
	DefaultBucketName           = "xwal"
	DefaultSegmentsObjectSizeMB = 1000
	DefaultSegmentsBucketSizeGB = 20
	DefaultCleanLogsInterval    = 1 * time.Minute
	DefaultProfile              = "default"
	DefaultRegion               = "us-east-1"
)

type AWSS3Config struct {
	// Name of the s3 bucket where WAL files will be stored
	BucketName string `yaml:"bucketName"`

	// AWS Region where the bucket is located
	Region string `yaml:"region"`

	// AWS Profile to be used (optional)
	Profile string `yaml:"profile,omitempty"`

	// AWS Auth credentials (optional). Default is to read from aws credentials file
	Auth *S3Auth `yaml:"auth,omitempty"`

	// Size in megabytes of each object inside the s3 WAL bucket
	SegmentsObjectSizeMB int `yaml:"segmentsObjectSizeMB"`

	// Size in Gigabytes of the Segments bucket inside s3
	SegmentsBucketSizeGB uint32 `yaml:"segmentsBucketSizeGB"`

	// Interval to clean garbage logs
	CleanLogsInterval time.Duration `yaml:"cleanLogsInterval"`

	// Reference to zap logger. Does not get informed by yaml files. It is set by the xwal package
	Logger *zap.Logger

	// Reference to the AWS Session
	AWSConfig *aws.Config
}

// S3Auth represents the AWS Auth credentials
type S3Auth struct {
	AccessKey string `yaml:"accessKey"`
	SecretKey string `yaml:"secretKey"`
}

// DefaultAWSS3Config returns a default configuration for the AWSS3Config
func DefaultAWSS3Config() *AWSS3Config {
	return &AWSS3Config{
		BucketName:           DefaultBucketName,
		Region:               DefaultRegion,
		Profile:              DefaultProfile,
		SegmentsObjectSizeMB: DefaultSegmentsObjectSizeMB,
		SegmentsBucketSizeGB: DefaultSegmentsBucketSizeGB,
		CleanLogsInterval:    DefaultCleanLogsInterval,
	}
}
