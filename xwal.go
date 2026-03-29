package xwal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/pantuza/xwal/internal/buffer"
	"github.com/pantuza/xwal/internal/logs"
	"github.com/pantuza/xwal/internal/telemetry"
	"github.com/pantuza/xwal/pkg/backends/awss3"
	"github.com/pantuza/xwal/pkg/backends/localfs"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// The replay callback function signature
type ReplayCallbackFunc func([]*xwalpb.WALEntry) error

// XWAL is the main struct that implements the WAL and control
// the lifecycle of the WAL. It is responsible to control the WAL Backend
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

	// Logger reference
	logger *zap.Logger

	tel           *telemetry.Telemetry
	obsUnregister func() error
}

func NewXWAL(cfg *XWALConfig) (*XWAL, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid xwal config: %w", err)
	}

	logger, err := logs.NewLogger(cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("creating logger: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	telCfg := telemetry.Config{}
	if cfg.Telemetry != nil {
		telCfg.Disabled = cfg.Telemetry.Disabled
		telCfg.MeterProvider = cfg.Telemetry.MeterProvider
		telCfg.TracerProvider = cfg.Telemetry.TracerProvider
	}
	tel, err := telemetry.New(telCfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("telemetry: %w", err)
	}

	wal := &XWAL{
		cfg:           cfg,
		lock:          sync.RWMutex{},
		wg:            sync.WaitGroup{},
		closed:        false,
		ctx:           ctx,
		cancel:        cancel,
		FlushInterval: time.NewTicker(cfg.FlushFrequency),
		buffer:        buffer.NewInMemoryBuffer(cfg.BufferSize, cfg.BufferEntriesLength),
		logger:        logger,
		tel:           tel,
	}

	if err := wal.loadBackend(); err != nil {
		cancel()
		if wal.backend != nil {
			_ = wal.backend.Close()
		}
		return nil, err
	}

	unreg, err := tel.RegisterObservers(wal.observeMetricsSnapshot)
	if err != nil {
		cancel()
		if wal.backend != nil {
			_ = wal.backend.Close()
		}
		return nil, fmt.Errorf("telemetry observers: %w", err)
	}
	wal.obsUnregister = unreg

	go wal.PeriodicFlush()
	wal.osSignalHandler()

	return wal, nil
}

func (wal *XWAL) observeMetricsSnapshot() (entries, bytes, lsn, segment int64) {
	wal.lock.RLock()
	defer wal.lock.RUnlock()
	if wal.buffer != nil {
		e, b := wal.buffer.Stats()
		entries = int64(e)
		bytes = b
	}
	if wal.backend != nil {
		lsn = int64(wal.backend.LastIndex())
		if s, ok := wal.backend.(types.WALSegmentIndexer); ok {
			segment = int64(s.CurrentSegmentIndex())
		}
	}
	return entries, bytes, lsn, segment
}

func (wal *XWAL) backendType() types.WALBackendType {
	if wal.backend == nil {
		return types.WALBackendType("none")
	}
	return wal.backend.Type()
}

func (wal *XWAL) loadBackend() error {
	switch wal.cfg.WALBackend {
	case types.LocalFileSystemWALBackend:
		wal.cfg.BackendConfig.LocalFS.Logger = wal.logger
		wal.backend = localfs.NewLocalFSWALBackend(wal.cfg.BackendConfig.LocalFS)
	case types.AWSS3WALBackend:
		wal.cfg.BackendConfig.AWSS3.Logger = wal.logger
		wal.backend = awss3.NewAWSS3WALBackend(wal.cfg.BackendConfig.AWSS3)
	default:
		wal.backend = nil
	}

	if wal.backend == nil {
		wal.logger.Error("Unknown or unsupported WAL backend", zap.String("walBackend", string(wal.cfg.WALBackend)))
		return nil
	}

	if err := wal.backend.Open(); err != nil {
		wal.logger.Error("Error opening WAL backend", zap.Error(err))
		return fmt.Errorf("opening WAL backend: %w", err)
	}
	return nil
}

func (wal *XWAL) PeriodicFlush() {
	for {
		select {
		case <-wal.FlushInterval.C:
			wal.lock.Lock()
			if err := wal.flushToBackend(context.Background(), "periodic"); err != nil {
				wal.logger.Error("Error running PeriodicFlush", zap.Error(err))
			}
			wal.lock.Unlock()

		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *XWAL) Write(data []byte) error {
	return wal.WriteContext(context.Background(), data)
}

// WriteContext appends data to the WAL using ctx for trace propagation when the host
// application has configured a global or custom OpenTelemetry TracerProvider.
func (wal *XWAL) WriteContext(ctx context.Context, data []byte) (err error) {
	if wal.closed {
		return fmt.Errorf("xwal is closed: no more writes are allowed")
	}

	ctx, span := wal.tel.StartWriteSpan(ctx)
	start := time.Now()
	defer func() {
		wal.tel.RecordWrite(ctx, wal.backendType(), int64(len(data)), time.Since(start), err)
		telemetry.EndSpan(span, err)
	}()

	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.wg.Add(1)
	defer wal.wg.Done()

	entry, err := wal.createWALEntry(data)
	if err != nil {
		return fmt.Errorf("creating wal entry: %w", err)
	}

	return wal.writeOrFlush(ctx, entry)
}

func (wal *XWAL) createWALEntry(data []byte) (*xwalpb.WALEntry, error) {
	if wal.backend == nil {
		return nil, fmt.Errorf("wal backend not initialized: call NewXWAL to create a wal instance")
	}

	entry := &xwalpb.WALEntry{
		LSN:  wal.backend.LastIndex() + 1,
		Data: data,
	}

	var err error
	entry.CRC, err = entry.Checksum()
	if err != nil {
		return nil, fmt.Errorf("calculating checksum for new entry: %w", err)
	}

	wal.backend.IncLastIndex()
	return entry, nil
}

func (wal *XWAL) WriteBatch(entries []*xwalpb.WALEntry) error {
	return wal.WriteBatchContext(context.Background(), entries)
}

// WriteBatchContext appends pre-built entries using ctx for trace propagation.
func (wal *XWAL) WriteBatchContext(ctx context.Context, entries []*xwalpb.WALEntry) (err error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	n := len(entries)
	var payloadSum int64
	for _, e := range entries {
		payloadSum += int64(len(e.GetData()))
	}

	ctx, span := wal.tel.StartWriteBatchSpan(ctx, n)
	start := time.Now()
	defer func() {
		wal.tel.RecordWriteBatch(ctx, wal.backendType(), n, payloadSum, time.Since(start), err)
		telemetry.EndSpan(span, err)
	}()

	for _, entry := range entries {
		if err = wal.writeOrFlush(ctx, entry); err != nil {
			return err
		}
	}

	return nil
}

func (wal *XWAL) writeOrFlush(ctx context.Context, entry *xwalpb.WALEntry) error {
	// If the buffer is full, flush it
	if err := wal.buffer.Write(entry); err != nil {
		if errors.Is(err, buffer.ErrShouldFlushBuffer) {
			// Flushes the In Memory Buffer and Writes to the WAL Backend
			if err := wal.flushToBackend(ctx, "full"); err != nil {
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

func (wal *XWAL) flushToBackend(ctx context.Context, reason string) error {
	if wal.backend == nil {
		return nil
	}
	btyp := wal.backendType()
	wal.tel.RecordBufferFlush(ctx, reason, btyp)

	entriesToPersist := wal.buffer.Flush()
	n := len(entriesToPersist)
	var serialized int64
	for _, e := range entriesToPersist {
		serialized += int64(proto.Size(e))
	}

	fctx, fspan := wal.tel.StartFlushSpan(ctx, btyp, reason, n)
	var flushErr error
	defer func() {
		telemetry.EndSpan(fspan, flushErr)
	}()

	if n == 0 {
		return nil
	}

	bctx, bspan := wal.tel.StartBackendWriteSpan(fctx, btyp, n)
	start := time.Now()
	err := wal.backend.Write(entriesToPersist)
	dur := time.Since(start)
	wal.tel.RecordBackendWrite(bctx, btyp, n, serialized, dur, err)
	telemetry.EndSpan(bspan, err)
	if err != nil {
		wal.logger.Error("Error flushing to backend", zap.Error(err))
		flushErr = err
		return err
	}

	return nil
}

// CreateCheckpoint creates a checkpoint on the WAL. Checkpoints are simply suffixes on the segments files.
// Checkpoints will be used for Replaying the WAL from or to a given checkpoint.
// The method returns the checkpoint segments file number so the user can store it for later replay from that particular checkpoint further.
func (wal *XWAL) CreateCheckpoint() (checkpoint uint64, err error) {
	if wal.closed {
		return 0, fmt.Errorf("xwal is closed: no more checkpoints are allowed")
	}

	wal.lock.Lock()
	defer wal.lock.Unlock()

	ctx, span := wal.tel.StartCheckpointSpan(context.Background(), wal.backendType())
	defer func() {
		if err == nil {
			span.SetAttributes(attribute.Int64("xwal.checkpoint.id", int64(checkpoint)))
			wal.tel.RecordCheckpoint(ctx, wal.backendType())
		}
		telemetry.EndSpan(span, err)
	}()

	checkpoint, err = wal.backend.CreateCheckpoint()
	return checkpoint, err
}

func (wal *XWAL) runReplay(
	ctx context.Context,
	mode string,
	extra []attribute.KeyValue,
	batchSize int,
	callback ReplayCallbackFunc,
	backendRun func(ch chan *xwalpb.WALEntry) error,
) (rerr error) {
	if wal.closed {
		return fmt.Errorf("xwal is closed: no more replays are allowed")
	}

	btyp := wal.backendType()
	ctx, span := wal.tel.StartReplaySpan(ctx, btyp, mode, extra...)
	start := time.Now()
	wal.tel.RecordReplayStart(ctx, btyp, mode)
	defer func() {
		wal.tel.RecordReplayDuration(ctx, btyp, mode, time.Since(start), rerr)
		telemetry.EndSpan(span, rerr)
	}()

	wal.lock.Lock()
	defer wal.lock.Unlock()

	if err := wal.flushToBackend(ctx, "replay"); err != nil {
		return fmt.Errorf("flush before replay: %w", err)
	}

	channel := make(chan *xwalpb.WALEntry, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go wal.replayEntriesUsingUserCallback(channel, batchSize, callback, &wg)

	if err := backendRun(channel); err != nil {
		wal.abortReplayConsumer(channel, &wg)
		rerr = fmt.Errorf("replaying entries: %w", err)
		wal.tel.RecordBackendError(ctx, btyp, "replay")
		return rerr
	}
	close(channel)

	wg.Wait()
	return nil
}

// ReplayFromCheckpoint replays the WAL FROM a given checkpoint til the END of the WAL.
// It can be replayed backwards: from the end of the WAL til the given checkpoint.
// Entries read from the WAL Backend will be processed by the provided callback function.
func (wal *XWAL) ReplayFromCheckpoint(callback ReplayCallbackFunc, checkpoint uint64, backwards bool) error {
	return wal.runReplay(context.Background(), "from_checkpoint",
		[]attribute.KeyValue{
			attribute.Int64("xwal.checkpoint", int64(checkpoint)),
			attribute.Bool("xwal.replay.backwards", backwards),
		},
		wal.cfg.BufferEntriesLength, callback, func(ch chan *xwalpb.WALEntry) error {
			return wal.backend.ReplayFromCheckpoint(ch, checkpoint, backwards)
		})
}

// ReplayToCheckpoint replays the WAL from the BEGINNING of the WAL til the given checkpoint.
// It can be replayed backwards: from the given checkpoint til the beginning of the WAL.
// Entries read from the WAL Backend will be processed by the provided callback function.
func (wal *XWAL) ReplayToCheckpoint(callback ReplayCallbackFunc, checkpoint uint64, backwards bool) error {
	return wal.runReplay(context.Background(), "to_checkpoint",
		[]attribute.KeyValue{
			attribute.Int64("xwal.checkpoint", int64(checkpoint)),
			attribute.Bool("xwal.replay.backwards", backwards),
		},
		wal.cfg.BufferEntriesLength, callback, func(ch chan *xwalpb.WALEntry) error {
			return wal.backend.ReplayToCheckpoint(ch, checkpoint, backwards)
		})
}

func (wal *XWAL) Replay(callback ReplayCallbackFunc, batchSize int, backwards bool) error {
	return wal.runReplay(context.Background(), "full",
		[]attribute.KeyValue{attribute.Bool("xwal.replay.backwards", backwards)},
		batchSize, callback, func(ch chan *xwalpb.WALEntry) error {
			return wal.backend.Replay(ch, backwards)
		})
}

func (wal *XWAL) ReplayFromRange(callback ReplayCallbackFunc, batchSize int, backwards bool, start, end uint64) error {
	return wal.runReplay(context.Background(), "range",
		[]attribute.KeyValue{
			attribute.Int64("xwal.replay.start", int64(start)),
			attribute.Int64("xwal.replay.end", int64(end)),
			attribute.Bool("xwal.replay.backwards", backwards),
		},
		batchSize, callback, func(ch chan *xwalpb.WALEntry) error {
			return wal.backend.ReplayFromRange(ch, backwards, start, end)
		})
}

// abortReplayConsumer closes the replay channel and waits for the callback goroutine after a backend error.
func (wal *XWAL) abortReplayConsumer(channel chan *xwalpb.WALEntry, wg *sync.WaitGroup) {
	close(channel)
	wg.Wait()
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
					wal.logger.Error("Error replaying entries using user callback", zap.Error(err))
					wal.tel.RecordReplayCallbackError(context.Background(), wal.backendType())
				} else {
					wal.tel.RecordReplayEntries(context.Background(), wal.backendType(), int64(len(entries)))
				}

				wg.Done()
				return
			}

			entries = append(entries, entry)

			if len(entries) == batchSize {
				if err := callback(entries); err != nil {
					wal.logger.Error("Error calling user callback", zap.Error(err))
					wal.tel.RecordReplayCallbackError(context.Background(), wal.backendType())
				} else {
					wal.tel.RecordReplayEntries(context.Background(), wal.backendType(), int64(len(entries)))
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
		wal.logger.Error("Received system signal", zap.String("signal", sig.String()))
		if err := wal.Close(); err != nil {
			wal.logger.Error("closing xwal after signal", zap.Error(err))
		}
	}()
}

func (wal *XWAL) Close() error {
	if wal.closed {
		wal.logger.Warn("xWAL is already closed")
		return nil
	}

	defer func() {
		_ = wal.logger.Sync()
	}()

	if wal.obsUnregister != nil {
		_ = wal.obsUnregister()
		wal.obsUnregister = nil
	}

	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.logger.Info("Closing xWAL")

	// We must stop the interval before anything to avoid trying to flush after closing the backend
	wal.FlushInterval.Stop()
	wal.cancel()

	// Wait for all pending writes to finish
	wal.wg.Wait()

	if wal.backend != nil {
		if err := wal.flushToBackend(context.Background(), "close"); err != nil {
			wal.logger.Error("Error flushing to backend on close", zap.Error(err))
			wal.closed = true
			_ = wal.backend.Close()
			return err
		}
	}

	wal.closed = true // No more writes are allowed

	if wal.backend == nil {
		return nil
	}
	return wal.backend.Close()
}

// Flush persists any buffered entries to the WAL backend. Replay does this automatically
// before reading from storage; use Flush when you need durability without replay (e.g. before
// an external checkpoint).
func (wal *XWAL) Flush(ctx context.Context) error {
	if wal.closed {
		return fmt.Errorf("xwal is closed: flush not allowed")
	}
	wal.lock.Lock()
	defer wal.lock.Unlock()
	return wal.flushToBackend(ctx, "manual")
}

func (wal *XWAL) IsClosed() bool {
	return wal.closed
}
