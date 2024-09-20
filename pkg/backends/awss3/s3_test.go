package awss3

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
	"github.com/pantuza/xwal/internal/logs"
)

// Reusable configuration for tests
var cfg AWSS3Config

// init function to start localstack container and configure the AWS S3 client
// with the localstack endpoint. This function is called before the tests.
// Since the localstack container is started only once, the tests can be run
// multiple times and interact with the same container.
func init() {
	// Configures and starts localstack to use only the S3 service
	p := localstack.Preset(localstack.WithServices(localstack.S3))
	c, err := gnomock.Start(p)
	if err != nil {
		panic(err)
	}

	// Configures the logger to use the debug level during tests
	logger, err := logs.NewLogger("debug")
	if err != nil {
		panic(err)
	}

	// Configures the AWS S3 WAL Backend to communicate with the localstack container
	cfg = AWSS3Config{
		BucketName: "test-bucket",
		Region:     "sa-east-1",
		Auth: &S3Auth{
			AccessKey: "fakeAccessKey",
			SecretKey: "fakeSecretKey",
		},
		Logger: logger,
	}

	// Updates the S3 endpoint to localstack container address
	awsCfg, err := getAWSConfig(&cfg)
	if err != nil {
		panic(err)
	}
	s3Endpoint := fmt.Sprintf("http://%s/", c.Address(localstack.APIPort))
	awsCfg.BaseEndpoint = aws.String(s3Endpoint)

	cfg.AWSConfig = awsCfg
}

func TestCreateWALBucket(t *testing.T) {
	wal := NewAWSS3WALBackend(&cfg)
	err := wal.Open()
	assert.NoError(t, err)
}

func TestCreateWALBucketAlreadyExists(t *testing.T) {
	wal := NewAWSS3WALBackend(&cfg)
	err := wal.Open()
	assert.NoError(t, err)

	// The bucket should exists. Then, when we try to create it again, WAL implementation should not fail and simply reuse the existing bucket.
	if _, err := wal.s3Client.HeadBucket(wal.ctx, &s3.HeadBucketInput{Bucket: &wal.cfg.BucketName}); err == nil {
		err := wal.Open()
		assert.NoError(t, err)
	}
}
