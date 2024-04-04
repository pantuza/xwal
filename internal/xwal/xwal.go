package xwal

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/pantuza/xwal/internal/buffer"
	"github.com/pantuza/xwal/pkg/backends/localfs"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

type XWAL struct {
	// Configuration for the xWAL instance. It takes in consideration the backend config
	cfg *XWALConfig

	// Lock used to control concurrency reads and writes to the WAL Backend
	lock sync.RWMutex

	// WaitGroup to control the number of pending writes
	wg sync.WaitGroup

	// Flag to control if the xWAL is closed. No more writes are allowed when it becomes true
	closed bool

	// Context and cancel function to control the xWAL lifecycle
	ctx context.Context

	// Cancel function to stop the xWAL lifecycle
	cancel context.CancelFunc

	// Ticker to control the flush frequency
	FlushInterval *time.Ticker

	// In Memory Buffer to store entries before flushing to the backend
	buffer *buffer.InMemoryBuffer

	// Backend to store the WAL entries. It implements the WALBackendInterface
	backend types.WALBackendInterface
}

func NewXWAL(cfg *XWALConfig) (*XWAL, error) {
	ctx, cancel := context.WithCancel(context.Background())

	wal := &XWAL{
		cfg:           cfg,
		lock:          sync.RWMutex{},
		wg:            sync.WaitGroup{},
		closed:        false,
		ctx:           ctx,
		cancel:        cancel,
		FlushInterval: time.NewTicker(cfg.FlushFrequency),
		buffer:        buffer.NewInMemoryBuffer(cfg.BufferSize, cfg.BufferEntriesLength),
	}

	wal.loadBackend()
	go wal.PeriodicFlush()
	wal.osSignalHandler()

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

func (wal *XWAL) Write(data []byte) error {
	if wal.closed {
		return fmt.Errorf("xWAL is closed. No more writes are allowed.")
	}

	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.wg.Add(1)
	defer wal.wg.Done()

	entry, err := wal.createWALEntry(data)
	if err != nil {
		return fmt.Errorf("Error creating WALEntry: %v", err)
	}

	return wal.writeOrFlush(entry)
}

func (wal *XWAL) createWALEntry(data []byte) (*xwalpb.WALEntry, error) {
	if wal.backend == nil {
		return nil, fmt.Errorf("WAL backend not initialized. You must call NewXWAL to create a new WAL instance.")
	}

	entry := &xwalpb.WALEntry{
		LSN:  wal.backend.LastIndex() + 1,
		Data: data,
	}

	var err error
	entry.CRC, err = entry.Checksum()
	if err != nil {
		return nil, fmt.Errorf("Error calculating checksum while creating a new entry: %v", err)
	}

	wal.backend.IncLastIndex()
	return entry, nil
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
	if err := wal.buffer.Write(entry); err != nil {
		if err.Error() == buffer.ErrorShouldFlushBuffer {
			// Flushes the In Memory Buffer and Writes to the WAL Backend
			if err := wal.flushToBackend(); err != nil {
				return err
			}

			// Writes the current entry
			if err := wal.buffer.Write(entry); err != nil {
				return err // If we fail again, it seems we indeed have a problem
			}
		} else {
			return err
		}
	}

	return nil
}

func (wal *XWAL) flushToBackend() error {
	// Flushes the In Memory Buffer and Writes to the WAL Backend
	entriesToPersist := wal.buffer.Flush()

	// TODO: Asynchronously writes to the backend
	// go func() {
	if err := wal.backend.Write(entriesToPersist); err != nil {
		fmt.Println(err) // TODO: Log error properly
	}
	// }()

	return nil
}

func (wal *XWAL) Replay(callback func([]*xwalpb.WALEntry) error, batchSize int, backwards bool) error {
	wal.lock.RLock()
	defer wal.lock.RUnlock()

	channel := make(chan *xwalpb.WALEntry, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go wal.replayEntriesUsingUserCallback(channel, batchSize, callback, &wg)

	if err := wal.backend.Replay(channel, backwards); err != nil {
		return fmt.Errorf("Error replaying entries: %v", err)
	}
	close(channel)

	wg.Wait()
	return nil
}

// Async function to read batchSize entries from a channel and call the callback function
func (wal *XWAL) replayEntriesUsingUserCallback(channel chan *xwalpb.WALEntry, batchSize int, callback func([]*xwalpb.WALEntry) error, wg *sync.WaitGroup) {
	entries := make([]*xwalpb.WALEntry, 0, batchSize)
	for {
		select {
		case entry, ok := <-channel:
			if !ok { // No more entries left in the channel

				// If there are entries left, call the callback function
				if err := callback(entries); err != nil {
					fmt.Println(err)
				}

				wg.Done()
				return
			}

			entries = append(entries, entry)

			if len(entries) == batchSize {
				if err := callback(entries); err != nil {
					fmt.Println(err) // TODO: Log error properly
				} else {
					entries = make([]*xwalpb.WALEntry, 0, batchSize)
				}
			}

		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *XWAL) osSignalHandler() {
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals // Block until a signal is received
		fmt.Println("Received signal:", sig)
		wal.Close()
	}()
}

func (wal *XWAL) Close() error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	fmt.Println("Closing xWAL")

	// We must stop the interval before anything to avoid trying to flush after closing the backend
	wal.FlushInterval.Stop()
	wal.cancel()

	// Wait for all pending writes to finish
	wal.wg.Wait()

	wal.closed = true // Now xWAL is closed. No more writes are allowed

	return wal.backend.Close()
}
