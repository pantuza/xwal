package types

// WALSegmentIndexer is optionally implemented by WAL backends to expose the
// active segment index for observability (for example, asynchronous gauges).
type WALSegmentIndexer interface {
	CurrentSegmentIndex() uint64
}
