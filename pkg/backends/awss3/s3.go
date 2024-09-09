package awss3

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"go.uber.org/zap"
)

type AWSS3WALBackend struct {
	cfg *AWSS3Config

	ctx               context.Context
	cancel            context.CancelFunc
	waitGroup         sync.WaitGroup
	cleanLogsInterval *time.Ticker

	firstSegmentIndex uint32
	lastSegmentIndex  uint32

	// currentSegmentFile *os.File
	lastLSN uint64
	logger  *zap.Logger

	// AWS S3 client reference
	s3Client *s3.Client
}

func NewAWSS3WALBackend(cfg *AWSS3Config) *AWSS3WALBackend {
	if cfg.CleanLogsInterval == 0 {
		cfg.CleanLogsInterval = 1 * time.Minute // Default interval to clean garbage logs
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &AWSS3WALBackend{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		waitGroup:         sync.WaitGroup{},
		cleanLogsInterval: time.NewTicker(cfg.CleanLogsInterval),
		firstSegmentIndex: 0,
		lastSegmentIndex:  0,
		lastLSN:           0,
		logger:            cfg.Logger,
	}
}

func (s *AWSS3WALBackend) Type() types.WALBackendType {
	return types.AWSS3WALBackend
}

func (wal *AWSS3WALBackend) Open() error {
	if err := awsAuthenticate(wal.cfg); err != nil {
		return fmt.Errorf("failed to authenticate with AWS: %w", err)
	}

	wal.s3Client = s3.NewFromConfig(*wal.cfg.AWSConfig)

	// Check if the bucket exists
	if err := wal.createWalBucket(); err != nil {
		return fmt.Errorf("failed to open WAL bucket: %w", err)
	}

	return nil
}

// createWalBucket creates the bucket if it does not exist.
func (wal *AWSS3WALBackend) createWalBucket() error {
	// Create the bucket if it does not exists
	if _, err := wal.s3Client.HeadBucket(wal.ctx, &s3.HeadBucketInput{Bucket: &wal.cfg.BucketName}); err != nil {

		wal.logger.Info("Bucket does not exists. Creating it.", zap.String("bucket", wal.cfg.BucketName))
		_, err := wal.s3Client.CreateBucket(wal.ctx, &s3.CreateBucketInput{
			Bucket: &wal.cfg.BucketName,
			CreateBucketConfiguration: &awsTypes.CreateBucketConfiguration{
				LocationConstraint: awsTypes.BucketLocationConstraint(wal.cfg.Region),
			},
		})
		if err != nil {
			return fmt.Errorf("Failed to create bucket: %w", err)
		}
		// Otherwise, read the bucket
	} else {
		wal.logger.Info("Bucket already exists", zap.String("bucket", wal.cfg.BucketName))
	}

	return nil
}

func (wal *AWSS3WALBackend) Write(entries []*xwalpb.WALEntry) error {
	return nil
}

func (wal *AWSS3WALBackend) Replay(channel chan *xwalpb.WALEntry, backwards bool) error {
	return nil
}

func (wal *AWSS3WALBackend) ReplayFromRange(channel chan *xwalpb.WALEntry, backwards bool, start, end uint32) error {
	return nil
}

func (wal *AWSS3WALBackend) CreateCheckpoint() (uint64, error) {
	return 0, nil
}

func (wal *AWSS3WALBackend) ReplayFromCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error {
	return nil
}

func (wal *AWSS3WALBackend) ReplayToCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error {
	return nil
}

func (wal *AWSS3WALBackend) Close() error {
	return nil
}

func (wal *AWSS3WALBackend) LastIndex() uint64 {
	return 0
}

func (wal *AWSS3WALBackend) IncLastIndex() {
}
