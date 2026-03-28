package xwal

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pantuza/xwal/pkg/backends/awss3"
	"github.com/pantuza/xwal/pkg/backends/localfs"
	"github.com/pantuza/xwal/pkg/types"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	yaml "gopkg.in/yaml.v3"
)

const (
	XWALConfigDefaultFile = "xwal.yaml"
)

type WALBackendsConfigs struct {
	LocalFS *localfs.LocalFSConfig `yaml:"localfs,omitempty"`
	AWSS3   *awss3.AWSS3Config     `yaml:"awss3,omitempty"`
}

// The configuration of the xWAL. It will drive how the xWAL will use
// any WAL Backend implementation
type XWALConfig struct {
	// Path to the yaml configuration file
	ConfigFile string

	// Type of the WAL Backend to be used
	WALBackend types.WALBackendType `yaml:"walBackend"`

	// The backend configuration
	BackendConfig WALBackendsConfigs `yaml:"backends"`

	// Size of the in memory buffer in MB
	BufferSize float64 `yaml:"bufferSize"`

	// Number of entries allowed inside the in memory buffer
	BufferEntriesLength int `yaml:"bufferEntriesLength"`

	// Frequency that xWAL Flushes data from memory to target WAL Backend
	FlushFrequency time.Duration `yaml:"flushFrequency"`

	// Defines which log level should xWAL use (debug, info, warn, error)
	LogLevel string `yaml:"logLevel"`

	// Telemetry configures OpenTelemetry metrics and traces. When nil, telemetry is
	// enabled using the global OpenTelemetry MeterProvider and TracerProvider.
	Telemetry *TelemetryConfig `yaml:"telemetry,omitempty"`
}

// TelemetryConfig holds optional OpenTelemetry providers for library instrumentation.
// MeterProvider and TracerProvider are not loaded from YAML; set them in code so
// metrics scrape with the host application (for example, a shared Prometheus exporter).
type TelemetryConfig struct {
	Disabled bool `yaml:"disabled,omitempty"`

	MeterProvider  metric.MeterProvider `yaml:"-"`
	TracerProvider trace.TracerProvider `yaml:"-"`
}

// Creates a new XWALConfig from yaml file or default values
func NewXWALConfig(filename string) *XWALConfig {
	useDefaultName := filename == ""
	if useDefaultName {
		filename = XWALConfigDefaultFile
	}

	config, err := loadConfigFromFile(filename)
	if err != nil {
		// NewXWALConfig("") tries xwal.yaml; a missing file is the normal case in tests and
		// apps that configure entirely in code—avoid spamming the default logger.
		quiet := useDefaultName && errors.Is(err, os.ErrNotExist)
		if !quiet {
			log.Printf("xwal: could not load config file %q: %v; using defaults", filename, err)
		}
		config = loadDefaultConfigValues()
	}

	config.ConfigFile = filename
	return config
}

func loadDefaultConfigValues() *XWALConfig {
	return &XWALConfig{
		WALBackend:          types.LocalFileSystemWALBackend,
		BufferSize:          32,
		BufferEntriesLength: 10,
		FlushFrequency:      1 * time.Second,
		BackendConfig: WALBackendsConfigs{
			LocalFS: localfs.DefaultLocalFSConfig(),
			AWSS3:   awss3.DefaultAWSS3Config(),
		},
		LogLevel: "error",
	}
}

func (cfg *XWALConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("xwal config is required")
	}
	if cfg.BufferSize <= 0 {
		return fmt.Errorf("bufferSize must be greater than zero")
	}
	if cfg.BufferEntriesLength <= 0 {
		return fmt.Errorf("bufferEntriesLength must be greater than zero")
	}
	if cfg.FlushFrequency <= 0 {
		return fmt.Errorf("flushFrequency must be greater than zero")
	}
	if cfg.BackendConfig.LocalFS == nil {
		return fmt.Errorf("backends.localfs config is required")
	}
	if err := cfg.BackendConfig.LocalFS.Validate(); err != nil {
		return err
	}
	if cfg.BackendConfig.AWSS3 != nil {
		if err := cfg.BackendConfig.AWSS3.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Loads configuration from a YAML file. It applies default values for missing options.
func loadConfigFromFile(filename string) (*XWALConfig, error) {
	defaults := loadDefaultConfigValues()
	config := *defaults

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	// Unmarshal can nil nested pointers when yaml keys are omitted or explicitly null.
	// Keep defaults so partial configuration files behave predictably.
	if config.BackendConfig.LocalFS == nil {
		config.BackendConfig.LocalFS = defaults.BackendConfig.LocalFS
	}
	if config.BackendConfig.AWSS3 == nil {
		config.BackendConfig.AWSS3 = defaults.BackendConfig.AWSS3
	}

	return &config, nil
}
