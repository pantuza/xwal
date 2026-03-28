// Package telemetry holds OpenTelemetry metrics and traces for xwal.
package telemetry

import (
	"context"
	"time"

	"github.com/pantuza/xwal/pkg/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	instrumentationName = "github.com/pantuza/xwal"
)

// Default latency histogram bounds in seconds (write / flush / backend / replay).
var durationBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// Config configures the telemetry pipeline for a WAL instance.
type Config struct {
	Disabled       bool
	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
}

// Telemetry holds pre-registered instruments and a tracer.
type Telemetry struct {
	disabled bool
	meter    metric.Meter
	tracer   trace.Tracer

	writesTotal               metric.Int64Counter
	writeBytesTotal           metric.Int64Counter
	writeDuration             metric.Float64Histogram
	bufferFlushTriggers       metric.Int64Counter
	backendWriteBatches       metric.Int64Counter
	backendWriteEntries       metric.Int64Counter
	backendWriteBytes         metric.Int64Counter
	backendWriteDuration      metric.Float64Histogram
	backendErrors             metric.Int64Counter
	replaysTotal              metric.Int64Counter
	replayEntriesTotal        metric.Int64Counter
	replayDuration            metric.Float64Histogram
	replayCallbackErrorsTotal metric.Int64Counter
	checkpointsTotal          metric.Int64Counter
}

// New builds a Telemetry helper. When cfg.Disabled is true, all recording is a no-op
// using noop meter and tracer providers.
func New(cfg Config) (*Telemetry, error) {
	var mp metric.MeterProvider
	var tp trace.TracerProvider
	if cfg.Disabled {
		mp = metricnoop.NewMeterProvider()
		tp = tracenoop.NewTracerProvider()
	} else {
		mp = cfg.MeterProvider
		if mp == nil {
			mp = otel.GetMeterProvider()
		}
		tp = cfg.TracerProvider
		if tp == nil {
			tp = otel.GetTracerProvider()
		}
	}

	met := mp.Meter(instrumentationName)
	tr := tp.Tracer(instrumentationName)

	t := &Telemetry{disabled: cfg.Disabled, meter: met, tracer: tr}

	var err error
	t.writesTotal, err = met.Int64Counter("xwal.writes",
		metric.WithDescription("Number of WAL append operations"),
		metric.WithUnit("{write}"))
	if err != nil {
		return nil, err
	}
	t.writeBytesTotal, err = met.Int64Counter("xwal.write.bytes",
		metric.WithDescription("User payload bytes appended via Write"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	t.writeDuration, err = met.Float64Histogram("xwal.write.duration",
		metric.WithDescription("Duration of Write/WriteContext"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	if err != nil {
		return nil, err
	}
	t.bufferFlushTriggers, err = met.Int64Counter("xwal.buffer.flush.triggers",
		metric.WithDescription("In-memory buffer flush invocations by trigger reason"),
		metric.WithUnit("{flush}"))
	if err != nil {
		return nil, err
	}
	t.backendWriteBatches, err = met.Int64Counter("xwal.backend.write.batches",
		metric.WithDescription("Backend Write calls with at least one entry"),
		metric.WithUnit("{batch}"))
	if err != nil {
		return nil, err
	}
	t.backendWriteEntries, err = met.Int64Counter("xwal.backend.write.entries",
		metric.WithDescription("WAL entries persisted to the backend"),
		metric.WithUnit("{entry}"))
	if err != nil {
		return nil, err
	}
	t.backendWriteBytes, err = met.Int64Counter("xwal.backend.write.bytes",
		metric.WithDescription("Serialized protobuf bytes written to the backend"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	t.backendWriteDuration, err = met.Float64Histogram("xwal.backend.write.duration",
		metric.WithDescription("Duration of backend Write"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	if err != nil {
		return nil, err
	}
	t.backendErrors, err = met.Int64Counter("xwal.backend.errors",
		metric.WithDescription("Backend failures by operation"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}
	t.replaysTotal, err = met.Int64Counter("xwal.replays",
		metric.WithDescription("Replay operations started"),
		metric.WithUnit("{replay}"))
	if err != nil {
		return nil, err
	}
	t.replayEntriesTotal, err = met.Int64Counter("xwal.replay.entries",
		metric.WithDescription("Entries delivered to replay callbacks"),
		metric.WithUnit("{entry}"))
	if err != nil {
		return nil, err
	}
	t.replayDuration, err = met.Float64Histogram("xwal.replay.duration",
		metric.WithDescription("Wall-clock duration of replay API calls"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	if err != nil {
		return nil, err
	}
	t.replayCallbackErrorsTotal, err = met.Int64Counter("xwal.replay.callback.errors",
		metric.WithDescription("User replay callback errors"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}
	t.checkpointsTotal, err = met.Int64Counter("xwal.checkpoints",
		metric.WithDescription("Checkpoints created successfully"),
		metric.WithUnit("{checkpoint}"))
	if err != nil {
		return nil, err
	}

	return t, nil
}

// RegisterObservers registers asynchronous gauges for buffer state, last LSN, and segment index.
// The snapshot function must be concurrency-safe (for example, use the WAL read lock).
func (t *Telemetry) RegisterObservers(snapshot func() (entries, bytes, lsn, segment int64)) (unregister func() error, err error) {
	if t.disabled {
		return func() error { return nil }, nil
	}

	entriesG, err := t.meter.Int64ObservableGauge("xwal.buffer.entries",
		metric.WithDescription("Current entries in the in-memory WAL buffer"),
		metric.WithUnit("{entry}"))
	if err != nil {
		return nil, err
	}
	bytesG, err := t.meter.Int64ObservableGauge("xwal.buffer.size_bytes",
		metric.WithDescription("Approximate serialized size buffered in memory"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	lsnG, err := t.meter.Int64ObservableGauge("xwal.last_lsn",
		metric.WithDescription("Last log sequence number reported by the backend"),
		metric.WithUnit("{lsn}"))
	if err != nil {
		return nil, err
	}
	segG, err := t.meter.Int64ObservableGauge("xwal.segment.index",
		metric.WithDescription("Current WAL segment index when supported by the backend"),
		metric.WithUnit("{segment}"))
	if err != nil {
		return nil, err
	}

	reg, err := t.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		e, b, l, s := snapshot()
		o.ObserveInt64(entriesG, e)
		o.ObserveInt64(bytesG, b)
		o.ObserveInt64(lsnG, l)
		o.ObserveInt64(segG, s)
		return nil
	}, entriesG, bytesG, lsnG, segG)
	if err != nil {
		return nil, err
	}

	return reg.Unregister, nil
}

// BackendAttr returns a stable attribute for the WAL backend type.
func BackendAttr(backend types.WALBackendType) attribute.KeyValue {
	return attribute.String("xwal.backend", string(backend))
}

// EndSpan completes a span, recording an error when non-nil.
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// StartWriteSpan starts a span for a single append operation.
func (t *Telemetry) StartWriteSpan(ctx context.Context) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, "xwal.write")
}

// StartWriteBatchSpan starts a span for a batch append.
func (t *Telemetry) StartWriteBatchSpan(ctx context.Context, batchSize int) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, "xwal.write_batch",
		trace.WithAttributes(attribute.Int("xwal.batch.size", batchSize)))
}

// StartFlushSpan starts a span for moving buffered entries toward storage.
func (t *Telemetry) StartFlushSpan(ctx context.Context, backend types.WALBackendType, reason string, entryCount int) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, "xwal.flush",
		trace.WithAttributes(
			BackendAttr(backend),
			attribute.String("xwal.flush.reason", reason),
			attribute.Int("xwal.entries.count", entryCount),
		))
}

// StartBackendWriteSpan starts a span for backend Write.
func (t *Telemetry) StartBackendWriteSpan(ctx context.Context, backend types.WALBackendType, entries int) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, "xwal.backend.write",
		trace.WithAttributes(
			BackendAttr(backend),
			attribute.Int("xwal.entries.count", entries),
		))
}

// StartReplaySpan starts a span for a replay operation.
func (t *Telemetry) StartReplaySpan(ctx context.Context, backend types.WALBackendType, mode string, extra ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{BackendAttr(backend), attribute.String("xwal.replay.mode", mode)}
	attrs = append(attrs, extra...)
	return t.tracer.Start(ctx, "xwal.replay", trace.WithAttributes(attrs...))
}

// StartCheckpointSpan starts a span for checkpoint creation.
func (t *Telemetry) StartCheckpointSpan(ctx context.Context, backend types.WALBackendType) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, "xwal.checkpoint", trace.WithAttributes(BackendAttr(backend)))
}

// RecordWrite records metrics for one logical append (Write or WriteContext).
func (t *Telemetry) RecordWrite(ctx context.Context, backend types.WALBackendType, payloadBytes int64, dur time.Duration, err error) {
	b := BackendAttr(backend)
	res := attribute.String("xwal.result", "success")
	if err != nil {
		res = attribute.String("xwal.result", "error")
	}
	attrs := []attribute.KeyValue{b, res}
	t.writesTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.writeBytesTotal.Add(ctx, payloadBytes, metric.WithAttributes(b))
	t.writeDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))
}

// RecordWriteBatch records metrics for WriteBatch / WriteBatchContext.
func (t *Telemetry) RecordWriteBatch(ctx context.Context, backend types.WALBackendType, n int, payloadBytes int64, dur time.Duration, err error) {
	b := BackendAttr(backend)
	res := attribute.String("xwal.result", "success")
	if err != nil {
		res = attribute.String("xwal.result", "error")
	}
	attrs := []attribute.KeyValue{b, res, attribute.Int("xwal.batch.entries", n)}
	t.writesTotal.Add(ctx, int64(n), metric.WithAttributes(b, res))
	t.writeBytesTotal.Add(ctx, payloadBytes, metric.WithAttributes(b))
	t.writeDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))
}

// RecordBufferFlush counts a buffer flush trigger.
func (t *Telemetry) RecordBufferFlush(ctx context.Context, reason string, backend types.WALBackendType) {
	t.bufferFlushTriggers.Add(ctx, 1, metric.WithAttributes(
		attribute.String("xwal.flush.reason", reason),
		BackendAttr(backend),
	))
}

// RecordBackendWrite records backend persistence metrics and errors.
func (t *Telemetry) RecordBackendWrite(ctx context.Context, backend types.WALBackendType, entries int, serializedBytes int64, dur time.Duration, err error) {
	b := BackendAttr(backend)
	if entries > 0 {
		t.backendWriteBatches.Add(ctx, 1, metric.WithAttributes(b))
		t.backendWriteEntries.Add(ctx, int64(entries), metric.WithAttributes(b))
		t.backendWriteBytes.Add(ctx, serializedBytes, metric.WithAttributes(b))
	}
	t.backendWriteDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(b))
	if err != nil {
		t.backendErrors.Add(ctx, 1, metric.WithAttributes(b, attribute.String("xwal.operation", "write")))
	}
}

// RecordBackendError increments backend error counter for non-write operations.
func (t *Telemetry) RecordBackendError(ctx context.Context, backend types.WALBackendType, operation string) {
	t.backendErrors.Add(ctx, 1, metric.WithAttributes(
		BackendAttr(backend),
		attribute.String("xwal.operation", operation),
	))
}

// RecordReplayStart increments replay counter (call once per replay API).
func (t *Telemetry) RecordReplayStart(ctx context.Context, backend types.WALBackendType, mode string) {
	t.replaysTotal.Add(ctx, 1, metric.WithAttributes(
		BackendAttr(backend),
		attribute.String("xwal.replay.mode", mode),
	))
}

// RecordReplayDuration records end-to-end replay latency.
func (t *Telemetry) RecordReplayDuration(ctx context.Context, backend types.WALBackendType, mode string, dur time.Duration, err error) {
	res := attribute.String("xwal.result", "success")
	if err != nil {
		res = attribute.String("xwal.result", "error")
	}
	t.replayDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(
		BackendAttr(backend),
		attribute.String("xwal.replay.mode", mode),
		res,
	))
}

// RecordReplayEntries counts entries passed to the user callback successfully.
func (t *Telemetry) RecordReplayEntries(ctx context.Context, backend types.WALBackendType, n int64) {
	if n <= 0 {
		return
	}
	t.replayEntriesTotal.Add(ctx, n, metric.WithAttributes(BackendAttr(backend)))
}

// RecordReplayCallbackError counts user callback failures.
func (t *Telemetry) RecordReplayCallbackError(ctx context.Context, backend types.WALBackendType) {
	t.replayCallbackErrorsTotal.Add(ctx, 1, metric.WithAttributes(BackendAttr(backend)))
}

// RecordCheckpoint increments successful checkpoint counter.
func (t *Telemetry) RecordCheckpoint(ctx context.Context, backend types.WALBackendType) {
	t.checkpointsTotal.Add(ctx, 1, metric.WithAttributes(BackendAttr(backend)))
}
