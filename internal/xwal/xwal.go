package xwal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pantuza/xwal/internal/buffer"
	"github.com/pantuza/xwal/pkg/backends/localfs"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
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
		buffer:        buffer.NewInMemoryBuffer(cfg.BufferSize, cfg.BufferEntriesLength),
	}

	wal.loadBackend()
	go wal.PeriodicFlush()

	return wal, nil
}

func (wal *XWAL) loadBackend() {
	switch wal.cfg.WALBackend {
	case types.LocalFileSystemWALBackend:
		wal.backend = localfs.NewLocalFSWALBackend(wal.cfg.BackendConfig.LocalFS)
	case types.AWSS3WALBackend:
		wal.backend = nil
	default:
		wal.backend = nil
	}

	if err := wal.backend.Open(); err != nil {
		fmt.Println(err.Error())
	}
}

func (wal *XWAL) PeriodicFlush() {
	for {
		select {
		case <-wal.FlushInterval.C:
			wal.lock.Lock()
			if err := wal.flushToBackend(); err != nil {
				fmt.Println(err) // TODO: Log error properly
			}
			wal.lock.Unlock()

		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *XWAL) Write(entry *xwalpb.WALEntry) error {

	wal.lock.Lock()
	defer wal.lock.Unlock()

	return wal.writeOrFlush(entry)
}

func (wal *XWAL) WriteBatch(entries []*xwalpb.WALEntry) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	for _, entry := range entries {

		if err := wal.writeOrFlush(entry); err != nil {
			return err
		}
	}

	return nil
}

func (wal *XWAL) writeOrFlush(entry *xwalpb.WALEntry) error {

	// If the buffer is full, flush it
	if err := wal.buffer.Write(entry); err.Error() == buffer.ErrorShouldFlushBuffer {

		// Flushes the In Memory Buffer and Writes to the WAL Backend
		if err := wal.flushToBackend(); err != nil {
			return err
		}

		// Writes the current entry
		if err := wal.buffer.Write(entry); err != nil {
			return err // If we fail again, it seems we indeed have a problem
		}
	}

	return nil
}

func (wal *XWAL) flushToBackend() error {

	// Flushes the In Memory Buffer and Writes to the WAL Backend
	entriesToPersist := wal.buffer.Flush()

	// Asynchronously writes to the backend
	go func() {
		if err := wal.backend.Write(entriesToPersist); err != nil {
			fmt.Println(err) // TODO: Log error properly
		}
	}()

	return nil
}

func (wal *XWAL) Close() error {
	wal.cancel()
	wal.FlushInterval.Stop()
	return wal.backend.Close()
}
