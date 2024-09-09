package awss3

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
	"github.com/pantuza/xwal/internal/logs"
)

func TestCreateWALBucket(t *testing.T) {
	p := localstack.Preset(localstack.WithServices(localstack.S3))
	c, err := gnomock.Start(p)
	if err != nil {
		panic(err)
	}
	defer func() { _ = gnomock.Stop(c) }()
	s3Endpoint := fmt.Sprintf("http://%s/", c.Address(localstack.APIPort))

	logger, err := logs.NewLogger("debug")
	if err != nil {
		panic(err)
	}

	cfg := &AWSS3Config{
		BucketName: "test-bucket",
		Region:     "sa-east-1",
		Auth: &S3Auth{
			AccessKey: "fakeAccessKey",
			SecretKey: "fakeSecretKey",
		},
		Logger: logger,
	}
	awsCfg, err := getAWSConfig(cfg)
	if err != nil {
		panic(err)
	}
	awsCfg.BaseEndpoint = aws.String(s3Endpoint)

	cfg.AWSConfig = awsCfg

	wal := NewAWSS3WALBackend(cfg)
	err = wal.Open()
	if err != nil {
		panic(err)
	}
}
