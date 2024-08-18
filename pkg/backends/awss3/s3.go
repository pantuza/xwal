package awss3

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pantuza/xwal/pkg/types"
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

func (s *AWSS3WALBackend) Open() error {
	if err := awsAuthenticate(s.cfg); err != nil {
		return fmt.Errorf("failed to authenticate with AWS: %w", err)
	}
	return nil
}
