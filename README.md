
# :scroll: xWAL

<div align="center">

A Cross, thread-safe and buffered Write Ahead Log (WAL) library for Golang applications.

<img src="./assets/xwal-gopher.png" alt="xWAL Logo" width="200"/>

[![XWAL CI](https://github.com/pantuza/xwal/actions/workflows/main.yml/badge.svg)](https://github.com/pantuza/xwal/actions/workflows/main.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/pantuza/xwal)](https://pkg.go.dev/github.com/pantuza/xwal)
[![Go Report Card](https://goreportcard.com/badge/github.com/pantuza/xwal)](https://goreportcard.com/report/github.com/pantuza/xwal)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Library Latest Version](https://img.shields.io/badge/Package_Latest_Version-blue)](https://github.com/pantuza/xwal/releases)

</div>

* Cross? Yes, we mean you can choose your WAL Backend: Local Filesystem, AWS s3, etc.
* Thread-safe? Yes, once you have a xwal instance, you can safelly call its methods concurrently.
* Buffered? Yes, xwal uses an In Memory Buffer that flushes to the chosen WAL Backend asynchronosly.

## Requirements

- Go **1.26** or newer (see `go.mod`).

## Installation
```bash
go get github.com/pantuza/xwal
```

## Usage

```go
cfg := xwal.NewXWALConfig("") // Uses default configuration

xwal, err := xwal.NewXWAL(cfg) // Creates a new WAL instance
if err != nil {
  panic(err)
}
defer xwal.Close()

// Your WAL is ready to be used. Thus, elsewhere in your code you can call:
err := xwal.Write([]byte(`{"data": "any data in any format serialized to bytes you want to persist in the WAL"}`))
if err != nil {
  panic(err)
}

// Let's suppose your remote backend is failing and you need to Replay data from WAL to it.
// You simply provide xwal a callback function that speaks to your backend and xwal will
// make sure to give you the stored data in the order you want:
func myCallback(entries []*xwalpb.WALEntry) error {
  for _, entry := range entries {
    // Here you can send your data to your backend or do any computation you want
  }
  return nil
}

err = xwal.Replay(myCallback, 5, false) // Replay reads entries from the WAL and sends to your callback function
if err != nil {
  panic(err)
}
```

## Available Backends

| Backend | Description   | Examples   |
|-------------- | -------------- | -------------- |
| **Local FS**    | WAL entries are stored on the local filesystem     | [localfs](./examples/localfs/)     |
| **AWS s3**    | WAL entries are stored remotely on AWS s3 service    | [awss3](./examples/awss3/)  |


## Features

- Pluggable backends (local disk, Amazon S3) with a shared API.
- In-memory buffering with asynchronous flush to the backend.
- Safe concurrent use of a single `XWAL` instance from multiple goroutines.
- Replay with ordered delivery to a callback for recovery or reprocessing.
- Configurable segment sizing, buffer limits, logging (zap), and backend-specific options.

## Observability

xwal emits **OpenTelemetry metrics** (counters, histograms, and async gauges for buffer state, LSN, segment index) and **traces** (spans for append, flush, backend write, replay, and checkpoint). Your service decides how to export them: for example **OTel Metrics → Prometheus** on your existing `/metrics` endpoint, and **OTLP traces** to Jaeger, Tempo, or the OpenTelemetry Collector. By default, global OTel providers are no-op until you configure them; use **`WriteContext` / `WriteBatchContext`** to attach WAL spans under incoming request traces.

**Full reference:** metric names, attributes, span names, setup steps, and a runnable stdout demo are documented in [**OBSERVABILITY.md**](./OBSERVABILITY.md).

## Benchmarks

Micro-benchmarks live in [`benchmark/`](./benchmark/): they exercise the in-memory buffer (`BenchmarkWrite`), concurrent `XWAL.Write` calls (`BenchmarkConcurrentWrites`, each run uses an isolated temp WAL directory), and a small LocalFS write plus replay loop (`BenchmarkLocalFSReplay`). Run them with `make bench` (or `go test ./benchmark/ -bench .`); the Makefile runs five iterations per benchmark with the race detector enabled, matching CI-style checks.

```
 •  Running project benchmarks..
goos: darwin
goarch: arm64
pkg: github.com/pantuza/xwal/benchmark
cpu: Apple M2 Pro
BenchmarkWrite-10                        1297502               951.0 ns/op
BenchmarkWrite-10                        1334887               887.8 ns/op
BenchmarkWrite-10                        1351389               882.6 ns/op
BenchmarkWrite-10                        1357402               870.3 ns/op
BenchmarkWrite-10                        1349440               951.7 ns/op
BenchmarkConcurrentWrites-10                4315            256560 ns/op
BenchmarkConcurrentWrites-10                4444            262845 ns/op
BenchmarkConcurrentWrites-10                4240            261856 ns/op
BenchmarkConcurrentWrites-10                4687            262923 ns/op
BenchmarkConcurrentWrites-10                4113            265323 ns/op
BenchmarkLocalFSReplay-10                    181           8716194 ns/op
BenchmarkLocalFSReplay-10                    117           9738984 ns/op
BenchmarkLocalFSReplay-10                    100          11584058 ns/op
BenchmarkLocalFSReplay-10                    110          10509325 ns/op
BenchmarkLocalFSReplay-10                    103          14094333 ns/op
✅ PASS
ok      github.com/pantuza/xwal/benchmark       30.240s
```

## Contributing

Fork the repository, create a branch, and open a pull request. Run `make check` before pushing so lint, tests, benchmarks, and examples all pass. After changing Go or CI tooling, let GitHub Actions finish on your branch before tagging a release. Install **golangci-lint v2** with the same toolchain as the module (`GOTOOLCHAIN=go1.26.0 go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4`) or run `make setup`. The Makefile prefers `$(go env GOPATH)/bin/golangci-lint` over an older system copy. Keep changes focused and update the changelog when the behavior or public API changes.

## License
* [MIT License](./LICENSE)

## Knowledge Base
* [Write Ahead Log](https://en.wikipedia.org/wiki/Write-ahead_logging)
* [xWAL examples](./examples)
