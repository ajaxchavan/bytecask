package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Log struct {
	*zap.Logger
	level zap.AtomicLevel
}

// NewLogger returns a logger with custom options.
func NewLogger() (*Log, error) {
	config := zap.NewProductionConfig()

	config.Level.SetLevel(zapcore.DebugLevel)

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return &Log{
		Logger: logger,
		level:  zap.NewAtomicLevelAt(zapcore.DebugLevel),
	}, nil
}
