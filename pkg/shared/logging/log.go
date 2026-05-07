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
	"os"

	zap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger returns a new zap.SugaredLogger.
// Log level can be overridden at runtime via the NUMAFLOW_LOG_LEVEL env var
// (accepts any zapcore level: debug, info, warn, error, dpanic, panic, fatal).
// NUMAFLOW_DEBUG=true selects the development preset (console encoder, debug level).
// NUMAFLOW_LOG_LEVEL overrides the level chosen by NUMAFLOW_DEBUG; invalid values are silently ignored.
func NewLogger() *zap.SugaredLogger {
	var config zap.Config
	debugMode, ok := os.LookupEnv("NUMAFLOW_DEBUG")
	if ok && debugMode == "true" {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}
	// NUMAFLOW_LOG_LEVEL overrides the level set by the preset above.
	// Invalid values are silently ignored so a typo in a manifest does not crash the pod.
	if lvlStr, ok := os.LookupEnv("NUMAFLOW_LOG_LEVEL"); ok {
		if lvl, err := zapcore.ParseLevel(lvlStr); err == nil {
			config.Level = zap.NewAtomicLevelAt(lvl)
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
