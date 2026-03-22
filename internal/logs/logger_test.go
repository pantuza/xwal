package logs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestNewLogger_knownLevels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		level   string
		debugOn bool
		infoOn  bool
	}{
		{"debug", true, true},
		{"info", false, true},
		{"warn", false, false},
		{"error", false, false},
	}

	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			t.Parallel()
			log, err := NewLogger(tc.level)
			require.NoError(t, err)
			require.NotNil(t, log)
			core := log.Core()
			assert.Equal(t, tc.debugOn, core.Enabled(zapcore.DebugLevel))
			assert.Equal(t, tc.infoOn, core.Enabled(zapcore.InfoLevel))
			assert.True(t, core.Enabled(zapcore.ErrorLevel))
		})
	}
}

func TestNewLogger_unknownLevelDefaultsToError(t *testing.T) {
	t.Parallel()

	log, err := NewLogger("not-a-real-level")
	require.NoError(t, err)
	require.NotNil(t, log)
	core := log.Core()
	assert.False(t, core.Enabled(zapcore.DebugLevel))
	assert.False(t, core.Enabled(zapcore.InfoLevel))
	assert.False(t, core.Enabled(zapcore.WarnLevel))
	assert.True(t, core.Enabled(zapcore.ErrorLevel))
}
