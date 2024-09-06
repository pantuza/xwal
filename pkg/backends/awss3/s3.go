package awss3

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	// s3Client *s3.Client
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

	// TODO: wal.s3Client = s3.New(wal.cfg.AWSConfig)
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
