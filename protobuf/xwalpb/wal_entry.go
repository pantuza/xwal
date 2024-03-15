package xwalpb

import (
	"fmt"
	"hash/crc32"

	"google.golang.org/protobuf/proto"
)

// This function implements a CRC checksum algorithm for a WAL entry.
// The checksum is calculated over the entire entry, except for the last 4 bytes,
// which are assumed to be the checksum itself.
// The implementation uses crc32 ChecksumIEEE as the CRC algorithm.
func (w *WALEntry) Checksum() (uint32, error) {

	marshaledProto, err := proto.Marshal(w)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal WAL entry: %v", err)
	}

	return crc32.ChecksumIEEE(marshaledProto), nil
}
