package xwal

import (
	"os"
	"testing"
	"time"

	"github.com/pantuza/xwal/pkg/backends/awss3"
	"github.com/pantuza/xwal/pkg/backends/localfs"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/stretchr/testify/assert"
)

// TestNewXWALConfigDefaults tests that the NewXWALConfig function returns a configuration with default values
// when no file is specified or the specified file does not exist.
func TestNewXWALConfigDefaults(t *testing.T) {
	config := NewXWALConfig("")

	// Assert default values
	assert.Equal(t, types.LocalFileSystemWALBackend, config.WALBackend)
	assert.Equal(t, 32.0, config.BufferSize)
	assert.Equal(t, 1*time.Second, config.FlushFrequency)
	assert.IsType(t, &localfs.LocalFSConfig{}, config.BackendConfig.LocalFS)

	localConfig := config.BackendConfig.LocalFS
	assert.Equal(t, "/tmp/xwal", localConfig.DirPath)
	assert.Equal(t, 1000, localConfig.SegmentsFileSizeMB)
}

// TestNewXWALConfigFromFile tests loading configuration values from a provided YAML file.
func TestNewXWALConfigFromFile(t *testing.T) {
	// Create a temporary YAML file with configuration.
	content := []byte(`
walBackend: "local_filesystem"
bufferSize: 64
bufferEntriesLength: 100
flushFrequency: 2s
backends:
  localfs:
    dirPath: "/tmp/test_xwal"
    segmentsFileSize: 2000
    segmentsDirSizeGB: 3
`)
	tmpFile, err := os.CreateTemp("", "test_xwal_config.yaml")
	assert.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }() // Clean up

	_, err = tmpFile.Write(content)
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	config := NewXWALConfig(tmpFile.Name())

	// Assert values loaded from file
	assert.Equal(t, 64.0, config.BufferSize)
	assert.Equal(t, 100, config.BufferEntriesLength)
	assert.Equal(t, 2*time.Second, config.FlushFrequency)

	localConfig := config.BackendConfig.LocalFS
	assert.Equal(t, "/tmp/test_xwal", localConfig.DirPath)
	assert.Equal(t, 2000, localConfig.SegmentsFileSizeMB)
	assert.Equal(t, uint32(3), localConfig.SegmentsDirSizeGB)
}

func TestLoadConfigFromFile_ApplyDefaultsToMissingFields(t *testing.T) {
	content := []byte(`
walBackend: "local_filesystem"
backends:
  localfs:
    dirPath: "/tmp/test_xwal_partial"
`)
	tmpFile, err := os.CreateTemp("", "test_xwal_config_partial.yaml")
	assert.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, tmpFile.Close())

	config := NewXWALConfig(tmpFile.Name())
	assert.Equal(t, "/tmp/test_xwal_partial", config.BackendConfig.LocalFS.DirPath)
	assert.Equal(t, 32.0, config.BufferSize)
	assert.Equal(t, 10, config.BufferEntriesLength)
	assert.Equal(t, 1*time.Second, config.FlushFrequency)
	assert.NotNil(t, config.BackendConfig.AWSS3)
	assert.Equal(t, awss3.DefaultBucketName, config.BackendConfig.AWSS3.BucketName)
}

func TestXWALConfigValidate(t *testing.T) {
	t.Run("valid defaults", func(t *testing.T) {
		cfg := loadDefaultConfigValues()
		assert.NoError(t, cfg.Validate())
	})

	t.Run("invalid flush frequency", func(t *testing.T) {
		cfg := loadDefaultConfigValues()
		cfg.FlushFrequency = 0
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid localfs size relation", func(t *testing.T) {
		cfg := loadDefaultConfigValues()
		cfg.BackendConfig.LocalFS.SegmentsFileSizeMB = 5000
		cfg.BackendConfig.LocalFS.SegmentsDirSizeGB = 1
		assert.Error(t, cfg.Validate())
	})
}
