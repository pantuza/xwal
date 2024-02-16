package xwal

import (
	"fmt"
	"os"
	"testing"
	"time"

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
	assert.Equal(t, 32, config.BufferSize)
	assert.Equal(t, 1*time.Second, config.FlushFrequency)
	assert.IsType(t, &localfs.LocalFSConfig{}, config.BackendConfig.LocalFS)

	localConfig := config.BackendConfig.LocalFS
	assert.Equal(t, "/tmp/xwal", localConfig.DirPath)
	assert.Equal(t, 2, localConfig.SegmentsSize)
	assert.Equal(t, 1000, localConfig.FileSize)
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
    segmentsSize: 4
    fileSize: 2000
`)
	tmpFile, err := os.CreateTemp("", "test_xwal_config.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name()) // Clean up

	_, err = tmpFile.Write(content)
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	fmt.Println(tmpFile.Name())
	config := NewXWALConfig(tmpFile.Name())

	// Assert values loaded from file
	assert.Equal(t, 64, config.BufferSize)
	assert.Equal(t, 100, config.BufferEntriesLength)
	assert.Equal(t, 2*time.Second, config.FlushFrequency)

	localConfig := config.BackendConfig.LocalFS
	assert.Equal(t, "/tmp/test_xwal", localConfig.DirPath)
	assert.Equal(t, 4, localConfig.SegmentsSize)
	assert.Equal(t, 2000, localConfig.FileSize)
}
