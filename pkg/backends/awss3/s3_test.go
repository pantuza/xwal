package awss3

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
	"github.com/pantuza/xwal/internal/logs"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"google.golang.org/protobuf/encoding/protodelim"
)

var _ types.WALSegmentIndexer = (*AWSS3WALBackend)(nil)

// Reusable configuration for integration tests (Localstack via gnomock).
var cfg AWSS3Config

// localstackUnavailable is true when Docker/Localstack could not be started (e.g. macOS CI without Docker).
var localstackUnavailable bool

func TestMain(m *testing.M) {
	p := localstack.Preset(localstack.WithServices(localstack.S3))
	c, err := gnomock.Start(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "awss3: skipping Localstack integration tests: %v\n", err)
		localstackUnavailable = true
		os.Exit(m.Run())
	}
	defer func() { _ = gnomock.Stop(c) }()

	logger, err := logs.NewLogger("debug")
	if err != nil {
		fmt.Fprintf(os.Stderr, "awss3: logger: %v\n", err)
		os.Exit(1)
	}

	cfg = AWSS3Config{
		BucketName: "test-bucket",
		Region:     "sa-east-1",
		Auth: &S3Auth{
			AccessKey: "fakeAccessKey",
			SecretKey: "fakeSecretKey",
		},
		Logger: logger,
	}

	awsCfg, err := getAWSConfig(&cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "awss3: AWS config: %v\n", err)
		os.Exit(1)
	}
	s3Endpoint := fmt.Sprintf("http://%s/", c.Address(localstack.APIPort))
	awsCfg.BaseEndpoint = aws.String(s3Endpoint)

	cfg.AWSConfig = awsCfg

	os.Exit(m.Run())
}

func skipWithoutLocalstack(t *testing.T) {
	t.Helper()
	if localstackUnavailable {
		t.Skip("Localstack not available (Docker required for integration tests)")
	}
}

func TestCreateWALBucket(t *testing.T) {
	skipWithoutLocalstack(t)
	wal := NewAWSS3WALBackend(&cfg)
	err := wal.Open()
	assert.NoError(t, err)
}

func TestCreateWALBucketAlreadyExists(t *testing.T) {
	skipWithoutLocalstack(t)
	wal := NewAWSS3WALBackend(&cfg)
	err := wal.Open()
	assert.NoError(t, err)

	// The bucket should exists. Then, when we try to create it again, WAL implementation should not fail and simply reuse the existing bucket.
	if _, err := wal.s3Client.HeadBucket(wal.ctx, &s3.HeadBucketInput{Bucket: &wal.cfg.BucketName}); err == nil {
		err := wal.Open()
		assert.NoError(t, err)
	}
}

func setupS3WAL(t *testing.T) *AWSS3WALBackend {
	t.Helper()
	skipWithoutLocalstack(t)

	testCfg := cfg
	testCfg.BucketName = fmt.Sprintf("test-bucket-%d", time.Now().UnixNano())
	testCfg.SegmentsObjectSizeMB = 10
	testCfg.SegmentsBucketSizeGB = 1

	wal := NewAWSS3WALBackend(&testCfg)
	assert.NoError(t, wal.Open())
	return wal
}

func TestWriteAppendsEntriesToCurrentObject(t *testing.T) {
	wal := setupS3WAL(t)

	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("a")}
	entry1.CRC, _ = entry1.Checksum()
	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("b")}
	entry2.CRC, _ = entry2.Checksum()

	assert.NoError(t, wal.Write([]*xwalpb.WALEntry{entry1}))
	assert.NoError(t, wal.Write([]*xwalpb.WALEntry{entry2}))

	obj, err := wal.s3Client.GetObject(wal.ctx, &s3.GetObjectInput{
		Bucket: &wal.cfg.BucketName,
		Key:    aws.String(fmt.Sprintf(S3WALSegmentObjectFormat, 0)),
	})
	assert.NoError(t, err)
	defer func() { _ = obj.Body.Close() }()

	var entries []*xwalpb.WALEntry
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(obj.Body)
	assert.NoError(t, err)
	reader := bytes.NewReader(buf.Bytes())
	for reader.Len() > 0 {
		entry := &xwalpb.WALEntry{}
		assert.NoError(t, protodelim.UnmarshalFrom(reader, entry))
		entries = append(entries, entry)
	}

	assert.Len(t, entries, 2)
	assert.Equal(t, uint64(1), entries[0].LSN)
	assert.Equal(t, uint64(2), entries[1].LSN)
}

func TestReplayReadsAndGarbageCollectsObjects(t *testing.T) {
	wal := setupS3WAL(t)

	entry1 := &xwalpb.WALEntry{LSN: 1, Data: []byte("one")}
	entry1.CRC, _ = entry1.Checksum()
	entry2 := &xwalpb.WALEntry{LSN: 2, Data: []byte("two")}
	entry2.CRC, _ = entry2.Checksum()
	assert.NoError(t, wal.Write([]*xwalpb.WALEntry{entry1, entry2}))

	channel := make(chan *xwalpb.WALEntry, 2)
	assert.NoError(t, wal.Replay(channel, false))
	close(channel)

	got := make([]*xwalpb.WALEntry, 0, 2)
	for e := range channel {
		got = append(got, e)
	}
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("one"), got[0].Data)
	assert.Equal(t, []byte("two"), got[1].Data)

	objects, err := wal.listWALObjects()
	assert.NoError(t, err)
	hasGarbage := false
	for _, o := range objects {
		if o.Key != nil && strings.HasSuffix(*o.Key, S3GarbageObjectExtension) {
			hasGarbage = true
			break
		}
	}
	assert.True(t, hasGarbage, "expected replayed object to be marked as garbage")
}

func TestCreateCheckpointAndReplayToCheckpoint(t *testing.T) {
	wal := setupS3WAL(t)

	entry := &xwalpb.WALEntry{LSN: 1, Data: []byte("cp")}
	entry.CRC, _ = entry.Checksum()
	assert.NoError(t, wal.Write([]*xwalpb.WALEntry{entry}))

	cp, err := wal.CreateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), cp)

	channel := make(chan *xwalpb.WALEntry, 2)
	assert.NoError(t, wal.ReplayToCheckpoint(channel, cp, false))
	close(channel)

	got := make([]*xwalpb.WALEntry, 0, 1)
	for e := range channel {
		got = append(got, e)
	}
	assert.Len(t, got, 1)
	assert.Equal(t, []byte("cp"), got[0].Data)
}
