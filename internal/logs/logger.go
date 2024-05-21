package logs

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(level string) *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(getLogLevel(level)),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Sampling:          nil,
		Encoding:          "json",
		EncoderConfig:     encoderCfg,
	}

	return zap.Must(config.Build())
}

func getLogLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel

	default:
		return zap.ErrorLevel
	}
}
