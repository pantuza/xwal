# xwal observability

xwal is instrumented with **OpenTelemetry** (OTel) **metrics** and **traces**. Signals use the instrumentation scope name **`github.com/pantuza/xwal`**.

Your application chooses **how** telemetry is exported: Prometheus (via an OTel metrics exporter), OTLP traces to a collector, stdout for debugging, and so on. xwal does not start its own HTTP server or collector.

---

## Enabling telemetry in your service

### Default behavior

If you never configure OpenTelemetry, the process-wide default `MeterProvider` and `TracerProvider` are **no-op**. xwal still creates instruments and tracers, but **nothing is exported**.

### Recommended: share the app’s OpenTelemetry SDK

1. During process startup, build the same **`MeterProvider`** and **`TracerProvider`** you use for the rest of the service (resource, exporters, sampling).
2. Register them globally **before** creating the WAL:

   ```go
   otel.SetMeterProvider(mp)
   otel.SetTracerProvider(tp)
   ```

3. Create xwal with `NewXWAL(cfg)` and leave **`cfg.Telemetry` nil** (or omit it). xwal uses `otel.GetMeterProvider()` and `otel.GetTracerProvider()`.

Metrics then appear on your chosen metrics pipeline (for example **OTel → Prometheus** on `/metrics`), and traces on your trace backend (for example **OTLP → Jaeger/Tempo**), alongside your other instrumentation.

### Explicit providers (optional)

To pass providers without globals:

```go
cfg.Telemetry = &xwal.TelemetryConfig{
    MeterProvider:  mp,
    TracerProvider: tp,
}
```

### Disabling instrumentation

For tests or special builds:

```go
cfg.Telemetry = &xwal.TelemetryConfig{Disabled: true}
```

### Linking traces to your request context

- **`Write`** and **`WriteBatch`** use `context.Background()` for tracing; WAL spans are usually **root** spans.
- Use **`WriteContext(ctx, data)`** and **`WriteBatchContext(ctx, entries)`** so `xwal.write` / `xwal.write_batch` can be **children** of your HTTP/gRPC (or other) spans when `ctx` carries the active span.

---

## Metrics reference

Instrument names use **dot** separators (OTel style). When you export to **Prometheus** via the OpenTelemetry Prometheus exporter, names are typically mapped to **underscores** (for example `xwal_writes_total`). Exact mapping depends on exporter version and configuration.

### Counters and histograms

| Instrument name | Type | Unit | Description |
|-----------------|------|------|-------------|
| `xwal.writes` | Counter | `{write}` | Append attempts (`Write` / `WriteContext` count 1; `WriteBatch` adds batch size). |
| `xwal.write.bytes` | Counter | `By` | User payload bytes (`Data` field) for append operations. |
| `xwal.write.duration` | Histogram | `s` | End-to-end duration of a single append or batch append (see attributes). |
| `xwal.buffer.flush.triggers` | Counter | `{flush}` | Each time the in-memory buffer is flushed toward the backend (`full` or `periodic`). |
| `xwal.backend.write.batches` | Counter | `{batch}` | Backend `Write` calls that persisted at least one entry. |
| `xwal.backend.write.entries` | Counter | `{entry}` | Total WAL entries passed to the backend in those writes. |
| `xwal.backend.write.bytes` | Counter | `By` | Serialized protobuf size written in backend writes. |
| `xwal.backend.write.duration` | Histogram | `s` | Duration of `backend.Write`. |
| `xwal.backend.errors` | Counter | `{error}` | Backend failures (`xwal.operation` = `write` or `replay`, etc.). |
| `xwal.replays` | Counter | `{replay}` | Replay API invocations started. |
| `xwal.replay.entries` | Counter | `{entry}` | Entries successfully delivered to the user replay callback. |
| `xwal.replay.duration` | Histogram | `s` | Wall-clock duration of each replay API call. |
| `xwal.replay.callback.errors` | Counter | `{error}` | User callback returned an error. |
| `xwal.checkpoints` | Counter | `{checkpoint}` | Checkpoints created successfully. |

### Asynchronous gauges

Updated on the reader’s collection interval (no extra labels on these instruments in xwal):

| Instrument name | Unit | Description |
|-----------------|------|-------------|
| `xwal.buffer.entries` | `{entry}` | Current number of entries in the in-memory buffer. |
| `xwal.buffer.size_bytes` | `By` | Approximate buffered size (same accounting as flush thresholds). |
| `xwal.last_lsn` | `{lsn}` | Last LSN from the backend (`LastIndex()`). |
| `xwal.segment.index` | `{segment}` | Current segment index when the backend implements `WALSegmentIndexer` (built-in LocalFS and S3 backends do). |

### Common metric attributes

| Attribute | Used on | Values / notes |
|-----------|---------|----------------|
| `xwal.backend` | Most instruments | `local_filesystem`, `aws_s3`, or `none` if the backend is not initialized. |
| `xwal.result` | `xwal.writes`, `xwal.write.duration`, `xwal.replay.duration` | `success` or `error`. |
| `xwal.flush.reason` | `xwal.buffer.flush.triggers` | `full` (buffer pressure) or `periodic` (ticker). |
| `xwal.operation` | `xwal.backend.errors` | e.g. `write`, `replay`. |
| `xwal.replay.mode` | Replay-related counters/histograms | `full`, `from_checkpoint`, `to_checkpoint`, `range`. |
| `xwal.batch.entries` | `xwal.write.duration` (batch path only) | Batch size passed to `WriteBatchContext`. |

Duration histograms use explicit buckets from **0.0005s** up to **10s** (see `internal/telemetry/telemetry.go`).

---

## Traces reference

Tracer name / instrumentation scope: **`github.com/pantuza/xwal`**.

### Span names and hierarchy

| Span name | When | Parent | Main attributes |
|-----------|------|--------|-----------------|
| `xwal.write` | Each `Write` / `WriteContext` | Caller’s span if `ctx` carries one; else root | (metrics carry backend/result; span has minimal attributes today) |
| `xwal.write_batch` | `WriteBatch` / `WriteBatchContext` | Same as above | `xwal.batch.size` |
| `xwal.flush` | Buffer flush before backend I/O | Often under `xwal.write` / `xwal.write_batch` when flush is synchronous from append | `xwal.backend`, `xwal.flush.reason` (`full` \| `periodic`), `xwal.entries.count` (entries in flush; 0 if empty) |
| `xwal.backend.write` | Each backend `Write` with entries | Child of `xwal.flush` | `xwal.backend`, `xwal.entries.count` |
| `xwal.replay` | `Replay`, `ReplayFromCheckpoint`, `ReplayToCheckpoint`, `ReplayFromRange` | Root for that call (uses `context.Background()` in these APIs) | `xwal.backend`, `xwal.replay.mode`, plus optional `xwal.checkpoint`, `xwal.replay.start`, `xwal.replay.end`, `xwal.replay.backwards` |
| `xwal.checkpoint` | `CreateCheckpoint` | Root for that call | `xwal.backend`; on success `xwal.checkpoint.id` |

On error, spans record the error and set status to **Error** where the implementation ends the span with a non-nil error (for example checkpoint failure, replay failure). Backend `Write` errors are still logged by xwal; **flush currently returns `nil`** for backward compatibility, but metrics and the `xwal.backend.write` span still reflect the error.

---

## Trying it locally

The repository includes an example that registers stdout exporters and global providers before creating xwal:

```bash
make run_telemetry_example
# or: go run ./examples/telemetry/
```

See [`examples/telemetry/main.go`](./examples/telemetry/main.go).

---

## Further reading

- [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/)
- [Metrics](https://opentelemetry.io/docs/languages/go/instrumentation/#metrics) and [traces](https://opentelemetry.io/docs/languages/go/instrumentation/#traces) instrumentation
- Prometheus: use an OpenTelemetry **Prometheus exporter** on your `MeterProvider` so xwal series appear on your existing scrape endpoint
