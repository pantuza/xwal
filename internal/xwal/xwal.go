package xwal

import (
	"context"
	"sync"
	"time"

	"github.com/pantuza/xwal/internal/buffer"
	"github.com/pantuza/xwal/pkg/types"
)

type XWAL struct {
	cfg           XWALConfig
	lock          sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	FlushInterval time.Ticker
	buffer        *buffer.InMemoryBuffer
	backend       types.WALBackendInterface
}

func NewXWAL(cfg XWALConfig) (*XWAL, error) {
	ctx, cancel := context.WithCancel(context.Background())

	wal := &XWAL{
		cfg:           cfg,
		lock:          sync.RWMutex{},
		ctx:           ctx,
		cancel:        cancel,
		FlushInterval: *time.NewTicker(cfg.FlushFrequency),
		buffer:        buffer.NewInMemoryBuffer(cfg.BufferSize, cfg.SegmentsSize),
	}

	wal.loadBackend()
	go wal.PeriodicFlush()

	return wal, nil
}

func (wal *XWAL) loadBackend() {
	switch wal.cfg.WALBackend {
	case types.LocalFileSystemWALBackend:
		wal.backend = localfs.Open()
	case types.AWSS3WALBackend:
		wal.backend = nil
	default:
		wal.backend = nil
	}
}

func (wal *XWAL) PeriodicFlush() {
	for {
		select {
		case <-wal.FlushInterval.C:
			wal.lock.Lock()
			wal.backend.Flush()
			wal.lock.Unlock()

		case <-wal.ctx.Done():
			return
		}
	}
}
