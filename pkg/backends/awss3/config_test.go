package awss3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAWSS3Config_Validate_errors(t *testing.T) {
	t.Parallel()

	t.Run("nil config", func(t *testing.T) {
		t.Parallel()
		var cfg *AWSS3Config
		assert.Error(t, cfg.Validate())
	})

	t.Run("empty required fields", func(t *testing.T) {
		t.Parallel()
		cfg := &AWSS3Config{}
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid segment sizes", func(t *testing.T) {
		t.Parallel()
		cfg := &AWSS3Config{
			BucketName:           "b",
			Region:               "us-east-1",
			SegmentsObjectSizeMB: 0,
			SegmentsBucketSizeGB: 1,
		}
		assert.Error(t, cfg.Validate())

		cfg.SegmentsObjectSizeMB = 1
		cfg.SegmentsBucketSizeGB = 0
		assert.Error(t, cfg.Validate())
	})
}

func TestAWSS3Config_Validate_defaultFromConstructor(t *testing.T) {
	t.Parallel()

	// NewAWSS3WALBackend fills defaults; Validate should pass on Open path.
	cfg := &AWSS3Config{}
	NewAWSS3WALBackend(cfg)
	require.NoError(t, cfg.Validate())
}

func TestDefaultAWSS3Config_Validate(t *testing.T) {
	t.Parallel()
	cfg := DefaultAWSS3Config()
	require.NoError(t, cfg.Validate())
}
