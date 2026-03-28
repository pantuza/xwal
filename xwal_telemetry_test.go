package xwal

import (
	"context"
	"testing"
	"time"

	"github.com/pantuza/xwal/protobuf/xwalpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func sumCounter(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "metric %s: want Sum[int64], got %T", name, m.Data)
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

func testWALConfigWithTelemetry(t *testing.T, dir string, mp *metric.MeterProvider) *XWALConfig {
	t.Helper()
	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	cfg.BufferSize = 64
	cfg.BufferEntriesLength = 100
	cfg.FlushFrequency = time.Hour
	cfg.Telemetry = &TelemetryConfig{
		MeterProvider:  mp,
		TracerProvider: tracenoop.NewTracerProvider(),
	}
	return cfg
}

func TestXWAL_WriteRecordsMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	dir := t.TempDir()
	cfg := testWALConfigWithTelemetry(t, dir, mp)

	wal, err := NewXWAL(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	require.NoError(t, wal.Write([]byte("telemetry")))

	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.writes"), int64(1))
	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.write.bytes"), int64(len("telemetry")))
}

func TestXWAL_FlushTriggersBackendMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	dir := t.TempDir()
	cfg := testWALConfigWithTelemetry(t, dir, mp)
	cfg.BufferEntriesLength = 1

	wal, err := NewXWAL(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	require.NoError(t, wal.Write([]byte("a")))
	require.NoError(t, wal.Write([]byte("b")))

	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.buffer.flush.triggers"), int64(1))
	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.backend.write.batches"), int64(1))
}

func TestXWAL_TelemetryDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := NewXWALConfig("")
	cfg.BackendConfig.LocalFS.DirPath = dir
	cfg.Telemetry = &TelemetryConfig{Disabled: true}

	wal, err := NewXWAL(cfg)
	require.NoError(t, err)
	require.NoError(t, wal.Write([]byte("x")))
	require.NoError(t, wal.Close())
}

func TestXWAL_CreateCheckpointRecordsMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	dir := t.TempDir()
	cfg := testWALConfigWithTelemetry(t, dir, mp)

	wal, err := NewXWAL(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	_, err = wal.CreateCheckpoint()
	require.NoError(t, err)

	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.checkpoints"), int64(1))
}

func TestXWAL_ReplayRecordsEntriesMetric(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	dir := t.TempDir()
	cfg := testWALConfigWithTelemetry(t, dir, mp)
	cfg.BufferEntriesLength = 1

	wal, err := NewXWAL(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	// Buffer size 1: third append flushes the second entry so at least two entries exist on disk before replay.
	require.NoError(t, wal.Write([]byte("r1")))
	require.NoError(t, wal.Write([]byte("r2")))
	require.NoError(t, wal.Write([]byte("r3")))

	err = wal.Replay(func(entries []*xwalpb.WALEntry) error {
		return nil
	}, 1, false)
	require.NoError(t, err)

	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.replay.entries"), int64(2))
	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.replays"), int64(1))
}

func TestXWAL_WriteBatchContextRecordsMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	dir := t.TempDir()
	cfg := testWALConfigWithTelemetry(t, dir, mp)

	wal, err := NewXWAL(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	entries := []*xwalpb.WALEntry{
		{LSN: 1, Data: []byte("x")},
	}
	require.NoError(t, wal.WriteBatchContext(context.Background(), entries))

	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.GreaterOrEqual(t, sumCounter(t, rm, "xwal.writes"), int64(1))
}
