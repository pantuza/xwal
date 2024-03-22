package xwalpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksum(t *testing.T) {
	entry := &WALEntry{
		LSN:  1,
		Data: []byte("test data"),
	}

	var err error
	entry.CRC, err = entry.Checksum()
	assert.NoError(t, err, "no error expected")
	assert.Equal(t, uint32(4049352028), entry.CRC, "expected checksum")
}

func TestChecksumIdempotency(t *testing.T) {
	entry := &WALEntry{
		LSN:  1,
		Data: []byte("test data"),
	}

	var err error
	entry.CRC, err = entry.Checksum()
	assert.NoError(t, err, "no error expected")
	assert.Equal(t, uint32(4049352028), entry.CRC, "expected checksum")

	// Calculate the checksum again.
	chksum, err := entry.Checksum()
	assert.NoError(t, err, "no error expected")
	assert.Equal(t, chksum, entry.CRC, "expected checksum")
}
