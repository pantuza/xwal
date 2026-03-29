# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.1] - 2026-03-29

### Fixed

- **xWAL:** Propagate backend write errors from buffer flush; return an error from `NewXWAL` when the backend `Open()` fails; flush the in-memory buffer before replay and on close; add `Flush(ctx)` for explicit durability.
- **Examples (AWS S3):** Clarify production AWS vs LocalStack (`XWAL_S3_ENDPOINT`); support default credential chain; exit with actionable hints on auth/IMDS failures instead of panicking.
- **Makefile:** `make help` includes targets whose names contain digits (for example `run_awss3_example`).

[0.6.1]: https://github.com/pantuza/xwal/releases/tag/v0.6.1

## [0.6.0] - 2026-03-28

### Changed (breaking)

- **Import path:** The stable public API is now the module root package `github.com/pantuza/xwal` (Go sources next to `go.mod`). The former `github.com/pantuza/xwal/internal/xwal` path is removed; update imports accordingly.

### Changed

- **Buffer:** Flush-needed condition is reported with sentinel error `buffer.ErrShouldFlushBuffer` and `errors.Is` instead of a string constant compared via `err.Error()`.

### Removed

- Unused `XWALError` / `XWALErrorTypes` types (nothing in the library returned them).

### Added

- **Docs:** Root [doc.go](doc.go) for pkg.go.dev, [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [BACKENDS.md](BACKENDS.md); README durability notes and positioning; dynamic GitHub release badge.
- **Tooling:** [.golangci.yml](.golangci.yml) for golangci-lint v2 (`linters.default: standard`), validated in CI.

### Fixed

- **Config:** When the YAML file cannot be loaded, fall back to defaults; log with the standard library `log` package only if the path was **explicit** or the error is not “file not found” for the default `xwal.yaml` used by `NewXWALConfig("")` (avoids noisy output in tests and code-only setups).

[0.6.0]: https://github.com/pantuza/xwal/releases/tag/v0.6.0

## [0.5.0] - 2026-03-28

### Added

- **Observability (OpenTelemetry):** Metrics (counters, histograms, asynchronous gauges) and traces for WAL appends, buffer flushes, backend writes, replay, checkpoints, and replay callback outcomes. Instrumentation scope `github.com/pantuza/xwal`; works with the process-wide OTel `MeterProvider` / `TracerProvider` or explicit `XWALConfig.Telemetry` (`MeterProvider`, `TracerProvider`, `Disabled`).
- **API:** `WriteContext`, `WriteBatchContext` for trace context propagation; `Write` / `WriteBatch` delegate to these with `context.Background()`.
- **Config:** `XWALConfig.Telemetry` and `TelemetryConfig` for optional providers or disabling instrumentation.
- **Backends:** `types.WALSegmentIndexer` and `CurrentSegmentIndex()` on LocalFS and AWS S3 backends for segment gauge support.
- **Buffer:** `InMemoryBuffer.Stats()` for observable buffer gauges.
- **Examples / tooling:** `examples/telemetry` and `make run_telemetry_example` (stdout metrics, stderr traces) for local verification.
- **Docs:** [OBSERVABILITY.md](./OBSERVABILITY.md) (full metric and span reference); README **Observability** section.
- **Dependencies:** OpenTelemetry Go SDK (`go.opentelemetry.io/otel` v1.29.0 and related modules, including stdout exporters for the telemetry example).

[0.5.0]: https://github.com/pantuza/xwal/releases/tag/v0.5.0

## [0.4.2] - 2026-03-27

### Added

- **Config validation:** Added `Validate` checks for `LocalFSConfig` and `XWALConfig` to reject invalid runtime settings early.

### Fixed

- **Config invariants:** Enforce segment size relation checks in both backends so a segment object/file cannot be larger than the configured bucket/directory capacity.
- **Config loading:** Ensure YAML loading starts from defaults and preserves missing nested backend defaults for partial configuration files.
- **Runtime safety:** Validate xWAL config during construction to fail fast on invalid values (for example zero flush frequency) instead of reaching runtime panics.

[0.4.2]: https://github.com/pantuza/xwal/releases/tag/v0.4.2

## [0.4.1] - 2026-03-25

### Fixed

- **Benchmarks:** Ensure `BenchmarkLocalFSReplay` closes the WAL each iteration so Windows can delete `b.TempDir()` trees during cleanup.

[0.4.1]: https://github.com/pantuza/xwal/releases/tag/v0.4.1

## [0.4.0] - 2026-03-25

### Changed (breaking)

- **API:** `WALBackendInterface.ReplayFromRange` and `XWAL.ReplayFromRange` now take `start` and `end` as `uint64` segment indices (previously `uint32`).
- **LocalFS / AWS S3:** Segment object and file names use the `wal_<n>` pattern (for example `wal_0`, `wal_42`); legacy zero-padded names such as `wal_00042` still parse when opening an existing WAL.

### Added

- **LocalFS / AWS S3:** `ErrSegmentIndexExhausted` when the segment counter cannot advance without wrapping.
- **Docs:** README **Benchmarks** section describing the `benchmark/` package and how to run `make bench`.

### Fixed

- **LocalFS:** Advance `firstSegmentIndex` after evicting the oldest segment when the configured directory size limit is exceeded; compute segment bounds from live segment and checkpoint files only so stale or removed `.garbage` files cannot desync indices; treat a missing segment during eviction (`ENOENT`) as already evicted and advance.
- **Benchmarks:** Use an isolated temporary WAL directory per benchmark run so results do not depend on shared `/tmp/xwal` state.

[0.4.0]: https://github.com/pantuza/xwal/releases/tag/v0.4.0

## [0.3.1] - 2026-03-22

### Fixed

- **CI:** Use `golangci-lint-action` **v7** so the requested **golangci-lint v2.11.4** installs correctly (v6 only supports golangci-lint v1).

[0.3.1]: https://github.com/pantuza/xwal/releases/tag/v0.3.1

## [0.3.0] - 2026-03-22

### Changed

- **Go:** Require Go 1.26 (module and CI updated from 1.22).
- **Lint:** CI and `make setup` use golangci-lint **v2.11.4**, built with Go 1.26 so analysis matches the module version; the Makefile prefers `$(go env GOPATH)/bin/golangci-lint` when present.

[0.3.0]: https://github.com/pantuza/xwal/releases/tag/v0.3.0

## [0.2.2] - 2026-03-22

### Fixed

- **LocalFS (Windows):** Close the active segment file before renaming it to garbage during replay; resolve path identity with `os.SameFile` when short vs long paths differ.
- **LocalFS:** Close the temporary read handle in `getLastLogSequencyNumber` (leaked handle blocked renames on Windows).
- **xWAL:** On backend replay error, close the replay channel and wait for the callback goroutine to avoid races.
- **Tests:** Close file handles in LocalFS tests before rename/delete; AWS S3 integration tests skip when Docker/Localstack is unavailable (e.g. macOS CI).

### Changed

- **CI:** Git checkout for releases uses full history (`fetch-depth: 0`) so GoReleaser changelogs include all commits.

[0.2.2]: https://github.com/pantuza/xwal/releases/tag/v0.2.2
