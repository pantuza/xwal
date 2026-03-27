package localfs

import (
	"testing"
	"time"

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

func TestLocalFSConfig_Validate(t *testing.T) {
	t.Parallel()

	t.Run("valid default config", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultLocalFSConfig()
		assert.NoError(t, cfg.Validate())
	})

	t.Run("nil config", func(t *testing.T) {
		t.Parallel()
		var cfg *LocalFSConfig
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid required fields", func(t *testing.T) {
		t.Parallel()
		cfg := &LocalFSConfig{}
		assert.Error(t, cfg.Validate())
	})

	t.Run("segment file larger than dir", func(t *testing.T) {
		t.Parallel()
		cfg := &LocalFSConfig{
			DirPath:            "/tmp/xwal",
			SegmentsFileSizeMB: 2049,
			SegmentsDirSizeGB:  2,
			CleanLogsInterval:  time.Second,
		}
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid clean interval", func(t *testing.T) {
		t.Parallel()
		cfg := &LocalFSConfig{
			DirPath:            "/tmp/xwal",
			SegmentsFileSizeMB: 1024,
			SegmentsDirSizeGB:  2,
		}
		assert.Error(t, cfg.Validate())
	})
}
