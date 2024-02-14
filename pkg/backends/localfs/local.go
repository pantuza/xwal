package localfs

import (
	"github.com/pantuza/xwal/protobuf/xwalpb"
)

type LocalFSBackend struct {
	cfg struct {
	}
}

func NewLocalFSWALBackend() *LocalFSBackend {
	return &LocalFSBackend{}
}

func (wal *LocalFSBackend) Open() error {
	return nil
}

func (wal *LocalFSBackend) Write(entry xwalpb.WALEntry) error {
	return nil
}

func (wal *LocalFSBackend) Read(index int64) (xwalpb.WALEntry, error) {
	return xwalpb.WALEntry{}, nil
}

func (wal *LocalFSBackend) Replay() ([]*xwalpb.WALEntry, error) {
	return nil, nil
}

func (wal *LocalFSBackend) Flush() error {
	return nil
}

func (wal *LocalFSBackend) Close() error {
	return nil
}

func (wal *LocalFSBackend) LastIndex() (int64, error) {
	return 0, nil
}
