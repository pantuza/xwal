package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pantuza/xwal/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func sumInt64Counter(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "expected Sum[int64] for %s, got %T", name, m.Data)
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

func gaugeInt64Value(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "expected Gauge[int64] for %s, got %T", name, m.Data)
			require.NotEmpty(t, g.DataPoints, "no data points for %s", name)
			return g.DataPoints[0].Value
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

func TestNew_Disabled(t *testing.T) {
	tel, err := New(Config{Disabled: true})
	require.NoError(t, err)
	ctx := context.Background()
	tel.RecordWrite(ctx, types.LocalFileSystemWALBackend, 10, time.Millisecond, nil)
	tel.RecordBackendWrite(ctx, types.LocalFileSystemWALBackend, 1, 100, time.Millisecond, errors.New("fail"))
	unreg, err := tel.RegisterObservers(func() (int64, int64, int64, int64) { return 1, 2, 3, 4 })
	require.NoError(t, err)
	require.NoError(t, unreg())
}

func TestRecordWrite_ManualReader(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	tel, err := New(Config{MeterProvider: mp, TracerProvider: tracenoop.NewTracerProvider()})
	require.NoError(t, err)

	ctx := context.Background()
	tel.RecordWrite(ctx, types.LocalFileSystemWALBackend, 128, 2*time.Millisecond, nil)
	tel.RecordWrite(ctx, types.LocalFileSystemWALBackend, 64, time.Millisecond, assert.AnError)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(2), sumInt64Counter(t, rm, "xwal.writes"))
	assert.Equal(t, int64(128+64), sumInt64Counter(t, rm, "xwal.write.bytes"))
}

func TestRecordBufferFlushAndBackendWrite_ManualReader(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	tel, err := New(Config{MeterProvider: mp, TracerProvider: tracenoop.NewTracerProvider()})
	require.NoError(t, err)
	ctx := context.Background()

	tel.RecordBufferFlush(ctx, "full", types.LocalFileSystemWALBackend)
	tel.RecordBackendWrite(ctx, types.LocalFileSystemWALBackend, 3, 900, 5*time.Millisecond, nil)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(1), sumInt64Counter(t, rm, "xwal.buffer.flush.triggers"))
	assert.Equal(t, int64(1), sumInt64Counter(t, rm, "xwal.backend.write.batches"))
	assert.Equal(t, int64(3), sumInt64Counter(t, rm, "xwal.backend.write.entries"))
	assert.Equal(t, int64(900), sumInt64Counter(t, rm, "xwal.backend.write.bytes"))
}

func TestRegisterObservers_Gauges(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	tel, err := New(Config{MeterProvider: mp, TracerProvider: tracenoop.NewTracerProvider()})
	require.NoError(t, err)

	unreg, err := tel.RegisterObservers(func() (int64, int64, int64, int64) {
		return 7, 4096, 99, 3
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = unreg() })

	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(7), gaugeInt64Value(t, rm, "xwal.buffer.entries"))
	assert.Equal(t, int64(4096), gaugeInt64Value(t, rm, "xwal.buffer.size_bytes"))
	assert.Equal(t, int64(99), gaugeInt64Value(t, rm, "xwal.last_lsn"))
	assert.Equal(t, int64(3), gaugeInt64Value(t, rm, "xwal.segment.index"))
}

func TestRecordReplayAndCheckpoint_ManualReader(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	tel, err := New(Config{MeterProvider: mp, TracerProvider: tracenoop.NewTracerProvider()})
	require.NoError(t, err)
	ctx := context.Background()

	tel.RecordReplayStart(ctx, types.LocalFileSystemWALBackend, "full")
	tel.RecordReplayDuration(ctx, types.LocalFileSystemWALBackend, "full", time.Millisecond, nil)
	tel.RecordReplayEntries(ctx, types.LocalFileSystemWALBackend, 5)
	tel.RecordReplayCallbackError(ctx, types.LocalFileSystemWALBackend)
	tel.RecordCheckpoint(ctx, types.LocalFileSystemWALBackend)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(1), sumInt64Counter(t, rm, "xwal.replays"))
	assert.Equal(t, int64(5), sumInt64Counter(t, rm, "xwal.replay.entries"))
	assert.Equal(t, int64(1), sumInt64Counter(t, rm, "xwal.replay.callback.errors"))
	assert.Equal(t, int64(1), sumInt64Counter(t, rm, "xwal.checkpoints"))
}

func TestEndSpan(t *testing.T) {
	tp := tracenoop.NewTracerProvider()
	tr := tp.Tracer("test")
	_, span := tr.Start(context.Background(), "s")
	EndSpan(span, errors.New("boom"))
	// noop span: no panic
	_, span2 := tr.Start(context.Background(), "ok")
	EndSpan(span2, nil)
}

func TestStartWriteSpanLinksTracer(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	tel, err := New(Config{MeterProvider: mp, TracerProvider: tracenoop.NewTracerProvider()})
	require.NoError(t, err)
	ctx, sp := tel.StartWriteSpan(context.Background())
	assert.NotNil(t, ctx)
	sp.End()
}
