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
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestNewLogger_DefaultLevel(t *testing.T) {
	t.Setenv("NUMAFLOW_DEBUG", "")
	t.Setenv("NUMAFLOW_LOG_LEVEL", "")
	logger := NewLogger()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewLogger_LogLevel(t *testing.T) {
	tests := []struct {
		envVal    string
		wantLevel zapcore.Level
	}{
		{"debug", zapcore.DebugLevel},
		{"info", zapcore.InfoLevel},
		{"warn", zapcore.WarnLevel},
		{"WARN", zapcore.WarnLevel},
		{"error", zapcore.ErrorLevel},
	}
	for _, tt := range tests {
		t.Run(tt.envVal, func(t *testing.T) {
			t.Setenv("NUMAFLOW_DEBUG", "")
			t.Setenv("NUMAFLOW_LOG_LEVEL", tt.envVal)
			logger := NewLogger()
			if logger == nil {
				t.Fatal("expected non-nil logger")
			}
			if !logger.Level().Enabled(tt.wantLevel) {
				t.Errorf("expected level %v to be enabled", tt.wantLevel)
			}
			if tt.wantLevel != zapcore.DebugLevel && logger.Level().Enabled(zapcore.DebugLevel) {
				t.Errorf("expected debug level to be disabled for %v", tt.envVal)
			}
		})
	}
}

func TestNewLogger_InvalidLogLevel_FallsBackToDefault(t *testing.T) {
	t.Setenv("NUMAFLOW_DEBUG", "")
	t.Setenv("NUMAFLOW_LOG_LEVEL", "garbage")
	logger := NewLogger()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	// default production config is info; debug should be disabled
	if logger.Level().Enabled(zapcore.DebugLevel) {
		t.Error("expected debug to be disabled on invalid NUMAFLOW_LOG_LEVEL")
	}
}

func TestNewLogger_LogLevelOverridesDebugPreset(t *testing.T) {
	// NUMAFLOW_DEBUG=true sets level to debug; NUMAFLOW_LOG_LEVEL=warn should win
	t.Setenv("NUMAFLOW_DEBUG", "true")
	t.Setenv("NUMAFLOW_LOG_LEVEL", "warn")
	logger := NewLogger()
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	if logger.Level().Enabled(zapcore.InfoLevel) {
		t.Error("expected info to be disabled when NUMAFLOW_LOG_LEVEL=warn overrides NUMAFLOW_DEBUG=true")
	}
	if !logger.Level().Enabled(zapcore.WarnLevel) {
		t.Error("expected warn to be enabled")
	}
}

func TestParseLogLevelRejectsRuntimeSpecificLevels(t *testing.T) {
	for _, level := range []string{"dpanic", "panic", "fatal"} {
		t.Run(level, func(t *testing.T) {
			if _, ok := parseLogLevel(level); ok {
				t.Fatalf("expected %q to be rejected", level)
			}
		})
	}
}
