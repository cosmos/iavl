package iavl

import (
	"log/slog"
	"os"
)

// Logger defines basic logger that IAVL expects.
// It is a subset of the cosmossdk.io/core/log.Logger interface.
// cosmossdk.io/log/log.Logger implements this interface.
type Logger interface {
	// Info takes a message and a set of key/value pairs and logs with level INFO.
	// The key of the tuple must be a string.
	Info(msg string, keyVals ...any)

	// Warn takes a message and a set of key/value pairs and logs with level WARN.
	// The key of the tuple must be a string.
	Warn(msg string, keyVals ...any)

	// Error takes a message and a set of key/value pairs and logs with level ERR.
	// The key of the tuple must be a string.
	Error(msg string, keyVals ...any)

	// Debug takes a message and a set of key/value pairs and logs with level DEBUG.
	// The key of the tuple must be a string.
	Debug(msg string, keyVals ...any)
}

// NewNopLogger returns a new logger that does nothing.
func NewNopLogger() Logger {
	return &noopLogger{}
}

type noopLogger struct{}

func (l *noopLogger) Info(string, ...any)  {}
func (l *noopLogger) Warn(string, ...any)  {}
func (l *noopLogger) Error(string, ...any) {}
func (l *noopLogger) Debug(string, ...any) {}

func NewTestLogger() Logger {
	return &testLogger{}
}

func NewDebugLogger() Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testLogger struct{}

func (l *testLogger) Info(msg string, keys ...any) {
	slog.Info(msg, keys...)
}
func (l *testLogger) Warn(msg string, keys ...any) {
	slog.Warn(msg, keys...)
}
func (l *testLogger) Error(msg string, keys ...any) {
	slog.Error(msg, keys...)
}
func (l *testLogger) Debug(msg string, keys ...any) {
	slog.Debug(msg, keys...)
}
