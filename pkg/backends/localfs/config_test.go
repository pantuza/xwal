package localfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultLocalFSConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultLocalFSConfig()
	assert.NotNil(t, cfg)
	assert.Equal(t, DefaultDirPath, cfg.DirPath)
	assert.Equal(t, DefaultSegmentsFileSizeMB, cfg.SegmentsFileSizeMB)
	assert.Equal(t, uint32(DefaultSegmentsDirSizeGB), cfg.SegmentsDirSizeGB)
	assert.Equal(t, DefaultCleanLogsInterval, cfg.CleanLogsInterval)
	assert.Nil(t, cfg.Logger)
}

func TestDefaultLocalFSConfig_isIndependent(t *testing.T) {
	t.Parallel()

	a := DefaultLocalFSConfig()
	b := DefaultLocalFSConfig()
	a.DirPath = "/tmp/other"
	assert.NotEqual(t, a.DirPath, b.DirPath, "each call should return a distinct struct")
}
