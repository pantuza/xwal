package xwal

import (
	"os"
	"time"

	"github.com/pantuza/xwal/pkg/backends/localfs"
	"github.com/pantuza/xwal/pkg/types"
	"gopkg.in/yaml.v3"
)

const (
	XWALConfigDefaultFile = "xwal.yaml"
)

// The configuration of the xWAL. It will drive how the xWAL will use
// any WAL Backend implementation
type XWALConfig struct {
	// Path to the yaml configuration file
	ConfigFile string

	// Type of the WAL Backend to be used
	WALBackend types.WALBackendType `yaml:"walBackend"`

	// The backend configuration
	BackendConfig interface{} `yaml:"backendConfig"`

	// Number of segments allowed inside the in memory buffer
	BufferSize int `yaml:"bufferSize"`

	// Number of entries allowed inside the in memory buffer
	BufferEntriesLength int `yaml:"bufferEntriesLength"`

	// Frequency that xWAL Flushes data from memory to target WAL Backend
	FlushFrequency time.Duration `yaml:"flushFrequency"`
}

// Creates a new XWALConfig from yaml file or default values
func NewXWALConfig(filename string) *XWALConfig {
	if filename != "" {
		filename = XWALConfigDefaultFile
	}

	config, err := loadConfigFromFile(filename)
	if err != nil {
		// TODO: Log to stdout we couldn't load the config file and will assume defaults
		config = loadDefaultConfigValues()
	}

	config.ConfigFile = filename
	return config
}

func loadDefaultConfigValues() *XWALConfig {
	return &XWALConfig{
		WALBackend:     types.LocalFileSystemWALBackend,
		BufferSize:     32,
		FlushFrequency: 1 * time.Second,
		BackendConfig: localfs.LocalFSConfig{
			DirPath:      "/tmp/xwal",
			SegmentsSize: 2,
			FileSize:     1000,
		},
	}
}

// Loads configuration from a YAML file. It applies default values for missing options.
func loadConfigFromFile(filename string) (*XWALConfig, error) {
	config := loadDefaultConfigValues()

	data, err := os.ReadFile(filename)
	if err != nil {
		// TODO: Log to stdout we couldn't READ the config file return config, nil
	}

	if err := yaml.Unmarshal(data, config); err != nil {
		// TODO: Log to stdout we couldn't Unmarshal the config file return config, nil
		return nil, err
	}

	return config, nil
}
