# Implementing a custom WAL backend

Backends implement [`types.WALBackendInterface`](pkg/types/wal_backend.go). The xwal core holds an in-memory buffer and calls your backend’s `Write` with batches of serialized entries when it flushes.

## Lifecycle

1. **`Open`** — Initialize storage, discover existing segments and the last LSN, and prepare for appends. Call before any other method.
2. **`Write`** — Persist the given entries immediately (fsync or remote PUT as your design requires). This is not the in-memory buffer; xwal has already decided to flush.
3. **`Close`** — Release resources.

## Segments and LSN

- Segment files follow the `wal_<n>` naming convention used by the built-in backends (see changelog for legacy name support).
- **`LastIndex` / `IncLastIndex`** — Coordinate monotonic entry identifiers with xwal when it builds new [`xwalpb.WALEntry`](protobuf/xwalpb/) values. If no entry has been written, `LastIndex` should return `0`.

## Replay

Replay methods receive a **channel** of `*xwalpb.WALEntry`. Send one entry at a time (or as your storage model allows); verify CRC before sending when reading from disk. Close the channel only when your implementation is done (xwal’s replay path coordinates with the consumer).

If you support segment indexing for observability, also implement [`WALSegmentIndexer`](pkg/types/segment_indexer.go) (`CurrentSegmentIndex`).

## Checkpoints

`CreateCheckpoint` renames or rotates the active segment so a suffix like `.checkpoint` appears on the segment file name. Replay-from/to checkpoint APIs rely on consistent naming with the built-in layout.

## Errors

Return errors from `Write` and replay methods when persistence fails. Note: today xwal’s internal flush path logs backend `Write` failures and records telemetry but may still return success from the flush helper for backward compatibility; see README **Durability** and [OBSERVABILITY.md](OBSERVABILITY.md).
