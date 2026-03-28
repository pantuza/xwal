// Package xwal provides a cross-backend, thread-safe, buffered write-ahead log (WAL) for Go.
//
// Import:
//
//	import "github.com/pantuza/xwal"
//
// Construct configuration with NewXWALConfig, then open the log with NewXWAL.
// Built-in backends are under pkg/backends/localfs and pkg/backends/awss3; custom
// backends implement pkg/types.WALBackendInterface (see BACKENDS.md in the repo root).
//
// OpenTelemetry metrics and traces: see OBSERVABILITY.md in the repository root.
package xwal
