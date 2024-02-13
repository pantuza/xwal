package xwal

import (
	"os"
	"time"

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

	// Name of the directory where WAL files will be stored
	DirName string `yaml:"dirName"`

	// Number of segments allowed inside the in memory buffer
	BufferSize int `yaml:"bufferSize"`

	// Frequency that xWAL Flushes data from memory to target WAL Backend
	FlushFrequency time.Duration `yaml:"flushFrequency"`

	// Size in megabytes of each segment inside files
	SegmentsSize int `yaml:"segmentSize"`

	// Size in megabytes of each file inside the WAL
	FileSize int `yaml:"fileSize"`
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
		DirName:        "xwal",
		BufferSize:     32,
		FlushFrequency: 1 * time.Second,
		SegmentsSize:   2,
		FileSize:       1000,
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
