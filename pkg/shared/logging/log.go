/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logging

import (
	"context"
	"fmt"
	"os"
	"strings"

	zap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	envDebug    = "NUMAFLOW_DEBUG"
	envLogLevel = "NUMAFLOW_LOG_LEVEL"
)

// NewLogger returns a new zap.SugaredLogger.
// Log level can be overridden at runtime via the NUMAFLOW_LOG_LEVEL env var
// (accepted values: debug, info, warn, error).
// NUMAFLOW_DEBUG=true selects the development preset (console encoder, debug level).
// NUMAFLOW_LOG_LEVEL overrides the level chosen by NUMAFLOW_DEBUG.
func NewLogger() *zap.SugaredLogger {
	var config zap.Config
	debugMode, ok := os.LookupEnv(envDebug)
	if ok && debugMode == "true" {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}
	// NUMAFLOW_LOG_LEVEL overrides the level set by the preset above.
	// Invalid values fall back to the preset level so a typo does not crash the pod.
	if lvlStr, ok := os.LookupEnv(envLogLevel); ok && strings.TrimSpace(lvlStr) != "" {
		if lvl, ok := parseLogLevel(lvlStr); ok {
			config.Level = zap.NewAtomicLevelAt(lvl)
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "invalid %s=%q, using default log level\n", envLogLevel, lvlStr)
		}
	}
	config.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	config.OutputPaths = []string{"stdout"}
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	return logger.Named("numaflow").Sugar()
}

func parseLogLevel(level string) (zapcore.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return zapcore.DebugLevel, true
	case "info":
		return zapcore.InfoLevel, true
	case "warn":
		return zapcore.WarnLevel, true
	case "error":
		return zapcore.ErrorLevel, true
	default:
		return zapcore.InfoLevel, false
	}
}

type loggerKey struct{}

// WithLogger returns a copy of parent context in which the
// value associated with logger key is the supplied logger.
func WithLogger(ctx context.Context, logger *zap.SugaredLogger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// FromContext returns the logger in the context.
func FromContext(ctx context.Context) *zap.SugaredLogger {
	if logger, ok := ctx.Value(loggerKey{}).(*zap.SugaredLogger); ok {
		return logger
	}
	return NewLogger()
}
