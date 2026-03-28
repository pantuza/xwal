// Example telemetry demonstrates wiring OpenTelemetry before creating xwal so
// metrics and traces are exported (here: JSON lines to stdout/stderr).
//
// Run: go run ./examples/telemetry/
//
// You should see:
//   - Trace spans named xwal.write, xwal.flush, xwal.backend.write, xwal.replay, …
//   - Periodic metric exports including xwal.writes, xwal.buffer.*, etc.
//
// The stock examples/simple app does not call otel.SetMeterProvider / SetTracerProvider,
// so the default global providers are no-op and nothing is exported.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pantuza/xwal"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	shutdown, err := setupOTel()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = shutdown() }()

	cfg := xwal.NewXWALConfig("")
	cfg.LogLevel = "error"
	cfg.BufferSize = 1
	cfg.BufferEntriesLength = 5

	dir, err := os.MkdirTemp("", "xwal-telemetry-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	cfg.BackendConfig.LocalFS.DirPath = dir

	// Telemetry field is nil: xwal uses the global MeterProvider and TracerProvider
	// configured in setupOTel().
	w, err := xwal.NewXWAL(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	fmt.Println("Writing a few entries (watch for xwal.* spans on stderr)…")
	for i := range 3 {
		if err := w.Write([]byte(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Replaying…")
	if err := w.Replay(func(entries []*xwalpb.WALEntry) error { return nil }, 1, false); err != nil {
		log.Fatal(err)
	}

	// Periodic metric reader may not have flushed yet; wait for export.
	time.Sleep(3 * time.Second)
	fmt.Println("\nDone. If you saw JSON above: traces on stderr (simple processor), metrics on stdout (periodic export).")
}

func setupOTel() (func() error, error) {
	ctx := context.Background()
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", "xwal-telemetry-example"),
		),
	)
	if err != nil {
		return nil, err
	}

	// Traces → stderr so they are easy to separate from metric JSON on stdout.
	texp, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(os.Stderr),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(texp)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	mexp, err := stdoutmetric.New(
		stdoutmetric.WithPrettyPrint(),
		stdoutmetric.WithWriter(os.Stdout),
	)
	if err != nil {
		return nil, err
	}
	mp := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(mexp, metric.WithInterval(2*time.Second))),
	)
	otel.SetMeterProvider(mp)

	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
		return mp.Shutdown(ctx)
	}, nil
}
