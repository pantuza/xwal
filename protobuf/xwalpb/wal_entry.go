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
	// Store the current CRC value and set it to 0 to calculate the Checksum.
	// It protects the CRC field from being included in the checksum calculation.
	storedCRC := w.CRC
	w.CRC = 0

	marshaledProto, err := proto.Marshal(w)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal WAL entry: %v", err)
	}

	// Restore the original CRC value.
	w.CRC = storedCRC

	return crc32.ChecksumIEEE(marshaledProto), nil
}
