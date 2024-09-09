package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/pantuza/xwal/internal/xwal"
	"github.com/pantuza/xwal/pkg/backends/awss3"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

func main() {
	fmt.Println("xWAL library")

	// lookup AWS Access key from environment variable
	awsAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if awsAccessKey == "" || awsSecretKey == "" {
		panic("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set")
	}

	s3Auth := &awss3.S3Auth{
		AccessKey: awsAccessKey,
		SecretKey: awsSecretKey,
	}

	cfg := xwal.NewXWALConfig("")
	cfg.LogLevel = "debug"
	cfg.BackendConfig.AWSS3.Region = "sa-east-1"
	cfg.BufferSize = 1
	cfg.BufferEntriesLength = 5
	cfg.WALBackend = types.AWSS3WALBackend
	cfg.BackendConfig.AWSS3.Auth = s3Auth

	xwal, err := xwal.NewXWAL(cfg)
	if err != nil {
		panic(err)
	}
	defer xwal.Close()

	for i := 0; i < 12; i++ {

		fmt.Printf("Writing entry %d\n", i)

		data := []byte(`{"name": "John", "age": 42, "city": "Belo Horizonte"}`) // any []byte fake data
		if err := xwal.Write(data); err != nil {
			panic(err)
		}
	}

	err = xwal.Replay(func(entries []*xwalpb.WALEntry) error {
		for _, entry := range entries {
			spew.Dump(entry)
		}
		return nil // Return nil or an appropriate error value
	}, 5, false)
	if err != nil {
		panic(err)
	}
	// spew.Dump(cfg.BackendConfig.AWSS3)
}
