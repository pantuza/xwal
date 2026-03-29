package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/davecgh/go-spew/spew"
	"github.com/pantuza/xwal"
	"github.com/pantuza/xwal/pkg/backends/awss3"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func main() {
	fmt.Println("xWAL library — AWS S3 backend example")

	cfg := xwal.NewXWALConfig("")
	cfg.LogLevel = "debug"
	cfg.BackendConfig.AWSS3.Region = "sa-east-1"
	cfg.BufferSize = 1
	cfg.BufferEntriesLength = 5
	cfg.WALBackend = types.AWSS3WALBackend

	endpoint := os.Getenv("XWAL_S3_ENDPOINT")
	awsAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	switch {
	case endpoint != "":
		fmt.Fprintf(os.Stderr, "\nUsing custom S3 endpoint (LocalStack or compatible), not production Amazon S3:\n  %s\n\n", endpoint)
		cfg.BackendConfig.AWSS3.AWSConfig = &aws.Config{}
		cfg.BackendConfig.AWSS3.AWSConfig.BaseEndpoint = aws.String(endpoint)
		switch {
		case awsAccessKey != "" && awsSecretKey != "":
			cfg.BackendConfig.AWSS3.Auth = &awss3.S3Auth{
				AccessKey: awsAccessKey,
				SecretKey: awsSecretKey,
			}
		case awsAccessKey == "" && awsSecretKey == "":
			// Common LocalStack dummy credentials
			cfg.BackendConfig.AWSS3.Auth = &awss3.S3Auth{AccessKey: "test", SecretKey: "test"}
			fmt.Fprintln(os.Stderr, "No AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY set; using test/test (typical for LocalStack).")
		default:
			fmt.Fprintln(os.Stderr, "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither for LocalStack defaults.")
			os.Exit(1)
		}

	default:
		bucket := cfg.BackendConfig.AWSS3.BucketName
		if bucket == "" {
			bucket = awss3.DefaultBucketName
		}
		fmt.Fprintf(os.Stderr, "\n*** This example uses the real Amazon S3 API (production AWS), not LocalStack. ***\n"+
			"It may create or use bucket %q in region %q and can incur charges.\n"+
			"Provide credentials via AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or use the default credential chain (profile, IAM role, etc.).\n"+
			"To run against LocalStack instead, set XWAL_S3_ENDPOINT (e.g. http://localhost:4566).\n\n",
			bucket, cfg.BackendConfig.AWSS3.Region)
		switch {
		case awsAccessKey != "" && awsSecretKey != "":
			cfg.BackendConfig.AWSS3.Auth = &awss3.S3Auth{
				AccessKey: awsAccessKey,
				SecretKey: awsSecretKey,
			}
		case awsAccessKey == "" && awsSecretKey == "":
			fmt.Fprintln(os.Stderr, "Using default AWS credential chain (no static keys in this environment).")
			fmt.Fprintln(os.Stderr, "Expect a configured profile or shared credentials (~/.aws/credentials), or an IAM role on AWS.")
			fmt.Fprintln(os.Stderr, "On a normal laptop, if nothing is configured the SDK may try EC2 instance metadata (IMDS), time out, and fail.")
		default:
			fmt.Fprintln(os.Stderr, "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither to use the default credential chain.")
			os.Exit(1)
		}
	}

	xwal, err := xwal.NewXWAL(cfg)
	if err != nil {
		exitAWSError("could not start xWAL (S3 backend failed to open)", err)
	}
	defer func() { _ = xwal.Close() }()

	for i := 0; i < 12; i++ {
		fmt.Printf("Writing entry %d\n", i)

		data := []byte(`{"name": "John", "age": 42, "city": "Belo Horizonte"}`) // any []byte fake data
		if err := xwal.Write(data); err != nil {
			exitError("write failed", err)
		}
	}

	err = xwal.Replay(func(entries []*xwalpb.WALEntry) error {
		for _, entry := range entries {
			spew.Dump(entry)
		}
		return nil
	}, 5, false)
	if err != nil {
		exitError("replay failed", err)
	}
}

func exitError(msg string, err error) {
	fmt.Fprintf(os.Stderr, "\nerror: %s\n", msg)
	fmt.Fprintf(os.Stderr, "detail: %v\n", err)
	os.Exit(1)
}

func exitAWSError(msg string, err error) {
	fmt.Fprintf(os.Stderr, "\nerror: %s\n", msg)
	fmt.Fprintf(os.Stderr, "detail: %v\n", err)
	if awsErrLooksLikeCredentialsOrNetwork(err) {
		fmt.Fprintf(os.Stderr, `
What usually fixes this:
  • Real AWS: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or run "aws configure" and use a profile (optionally AWS_PROFILE=...).
  • No AWS account for this demo: use LocalStack — export XWAL_S3_ENDPOINT=http://localhost:4566 (and start LocalStack) so no production credentials are needed.
  • IMDS / EC2 metadata timeouts on a dev machine: the SDK may still probe instance metadata; you can set AWS_EC2_METADATA_DISABLED=true to skip that, but you still need valid credentials or LocalStack as above.

`)
	}
	os.Exit(1)
}

func awsErrLooksLikeCredentialsOrNetwork(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "credential") ||
		strings.Contains(s, "authenticate") ||
		strings.Contains(s, "auth") && strings.Contains(s, "fail") ||
		strings.Contains(s, "imds") ||
		strings.Contains(s, "metadata") && strings.Contains(s, "ec2") ||
		strings.Contains(s, "security token") ||
		strings.Contains(s, "access key") ||
		strings.Contains(s, "no valid") && strings.Contains(s, "providers")
}
