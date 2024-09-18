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

package serverinfo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_SDKServerInfo(t *testing.T) {
	filepath := os.TempDir() + "/server-info"
	defer os.Remove(filepath)
	info := &ServerInfo{
		Protocol:               TCP,
		Language:               Java,
		MinimumNumaflowVersion: "1.3.0-rc1",
		Version:                "v0.8.0",
		Metadata:               map[string]string{"key1": "value1", "key2": "value2"},
	}
	err := write(info, WithServerInfoFilePath(filepath))
	assert.NoError(t, err)
	got, err := SDKServerInfo(WithServerInfoFilePath(filepath))
	assert.NoError(t, err)
	assert.Equal(t, info, got)
}

func Test_WaitUntilReady(t *testing.T) {
	serverInfoFile, err := os.CreateTemp("/tmp", "server-info")
	assert.NoError(t, err)
	defer os.Remove(serverInfoFile.Name())
	err = os.WriteFile(serverInfoFile.Name(), []byte("test"), 0644)
	assert.NoError(t, err)

	t.Run("test timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		err := waitUntilReady(ctx, WithServerInfoFilePath("/tmp/not-exist"))
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
	})

	t.Run("test success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		err = waitUntilReady(ctx, WithServerInfoFilePath(serverInfoFile.Name()))
		assert.NoError(t, err)
	})
}

func Test_ReadServerInfoFile(t *testing.T) {
	filepath := os.TempDir() + "/server-info"
	defer os.Remove(filepath)
	info := &ServerInfo{
		Protocol:               TCP,
		Language:               Java,
		MinimumNumaflowVersion: "1.3.0-rc1",
		Version:                "v0.8.0",
		Metadata:               map[string]string{"key1": "value1", "key2": "value2"},
	}
	err := write(info, WithServerInfoFilePath(filepath))
	assert.NoError(t, err)
	got, err := read(WithServerInfoFilePath("/tmp/not-exist"))
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
	assert.Nil(t, got)
	got, err = read(WithServerInfoFilePath(filepath))
	assert.NoError(t, err)
	assert.Equal(t, info, got)
}

func Test_CheckNumaflowCompatibility(t *testing.T) {
	tests := []struct {
		name               string
		numaflowVersion    string
		minNumaflowVersion string
		shouldErr          bool
		errMessage         string
	}{
		{
			name:               "Test with incompatible numaflow version, min is a stable version 1.1.7",
			numaflowVersion:    "v1.1.6",
			minNumaflowVersion: "1.1.7-z",
			shouldErr:          true,
			errMessage:         "numaflow version 1.1.6 must be upgraded to at least 1.1.7, in order to work with current SDK version",
		},
		{
			name:               "Test with compatible numaflow version - min is a stable version 1.1.6",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "1.1.6-z",
			shouldErr:          false,
		},
		{
			name:               "Test with incompatible numaflow version - min is a stable version 1.1.7, numaflow version is a pre-release version",
			numaflowVersion:    "v1.1.7-rc1",
			minNumaflowVersion: "1.1.7-z",
			shouldErr:          true,
			errMessage:         "numaflow version 1.1.7-rc1 must be upgraded to at least 1.1.7, in order to work with current SDK version",
		},
		{
			name:               "Test with compatible numaflow version - min is a stable version 1.1.6, numaflow version is a pre-release version",
			numaflowVersion:    "1.1.7-rc1",
			minNumaflowVersion: "1.1.6-z",
			shouldErr:          false,
		},
		{
			name:               "Test with incompatible numaflow version, min is a rc version 1.1.7-rc1",
			numaflowVersion:    "v1.1.6",
			minNumaflowVersion: "1.1.7-rc1",
			shouldErr:          true,
			errMessage:         "numaflow version 1.1.6 must be upgraded to at least 1.1.7-rc1, in order to work with current SDK version",
		},
		{
			name:               "Test with compatible numaflow version - min is a rc version 1.1.6-rc1",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "1.1.6-rc1",
			shouldErr:          false,
		},
		{
			name:               "Test with incompatible numaflow version - min is a rc version 1.1.7-rc2, numaflow version is a pre-release version",
			numaflowVersion:    "v1.1.7-rc1",
			minNumaflowVersion: "1.1.7-rc2",
			shouldErr:          true,
			errMessage:         "numaflow version 1.1.7-rc1 must be upgraded to at least 1.1.7-rc2, in order to work with current SDK version",
		},
		{
			name:               "Test with compatible numaflow version - min is a rc version 1.1.6-rc2, numaflow version is a pre-release version",
			numaflowVersion:    "1.1.6-rc2",
			minNumaflowVersion: "1.1.6-rc2",
			shouldErr:          false,
		},
		{
			name:               "Test with empty MinimumNumaflowVersion field",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "",
			shouldErr:          true,
			errMessage:         "server info does not contain minimum numaflow version. Upgrade to newer SDK version",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkNumaflowCompatibility(tt.numaflowVersion, tt.minNumaflowVersion)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
				assert.Contains(t, err.Error(), tt.errMessage)
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}

func Test_CheckSDKCompatibility(t *testing.T) {
	var testMinimumSupportedSDKVersions = sdkConstraints{
		Python: "0.6.0rc100",
		Go:     "0.6.0-z",
		Java:   "0.6.0-z",
		Rust:   "0.1.0-z",
	}
	tests := []struct {
		name                        string
		sdkVersion                  string
		sdkLanguage                 Language
		minimumSupportedSDKVersions sdkConstraints
		shouldErr                   bool
		errMessage                  string
	}{
		{
			name:                        "python pre-release version is lower than minimum supported version",
			sdkVersion:                  "v0.5.3a1",
			sdkLanguage:                 Python,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.5.3a1 must be upgraded to at least 0.6.0, in order to work with current numaflow version",
		},
		{
			name:                        "python pre-release version is compatible with minimum supported version",
			sdkVersion:                  "v0.6.3a1",
			sdkLanguage:                 Python,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
		},
		{
			name:                        "python stable release version is compatible with minimum supported version",
			sdkVersion:                  "v0.6.0",
			sdkLanguage:                 Python,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
		},
		{
			name:                        "python stable release version is lower than minimum supported version",
			sdkVersion:                  "v0.5.3",
			sdkLanguage:                 Python,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.5.3 must be upgraded to at least 0.6.0, in order to work with current numaflow version",
		},
		{
			name:                        "java release version is compatible with minimum supported version",
			sdkVersion:                  "v0.7.3",
			sdkLanguage:                 Java,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
		},
		{
			name:                        "golang rc release version is compatible with minimum supported version",
			sdkVersion:                  "v0.6.2-rc2",
			sdkLanguage:                 Go,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
		}, {
			name:                        "rust pre-release version is compatible with minimum supported version",
			sdkVersion:                  "v0.1.2-0.20240913163521-4910018031a7",
			sdkLanguage:                 Rust,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
		},
		{
			name:                        "rust release version is lower than minimum supported version",
			sdkVersion:                  "v0.0.3",
			sdkLanguage:                 Rust,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.0.3 must be upgraded to at least 0.1.0, in order to work with current numaflow version",
		},
		{
			name:                        "java rc release version is lower than minimum supported version",
			sdkVersion:                  "v0.6.0-rc1",
			sdkLanguage:                 Java,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.6.0-rc1 must be upgraded to at least 0.6.0, in order to work with current numaflow version",
		},
		{
			name:                        "golang pre-release version is lower than minimum supported version",
			sdkVersion:                  "v0.6.0-0.20240913163521-4910018031a7",
			sdkLanguage:                 Go,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.6.0-0.20240913163521-4910018031a7 must be upgraded to at least 0.6.0, in order to work with current numaflow version",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSDKCompatibility(tt.sdkVersion, tt.sdkLanguage, tt.minimumSupportedSDKVersions)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
				assert.Contains(t, err.Error(), tt.errMessage)
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}

// write is a test helper function to prepare server info file
func write(svrInfo *ServerInfo, opts ...Option) error {
	b, err := json.Marshal(svrInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal server info: %w", err)
	}
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	if err := os.Remove(options.serverInfoFilePath); !os.IsNotExist(err) && err != nil {
		return fmt.Errorf("failed to remove server-info file: %w", err)
	}
	f, err := os.Create(options.serverInfoFilePath)
	if err != nil {
		return fmt.Errorf("failed to create server-info file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(b)
	if err != nil {
		return fmt.Errorf("failed to write server-info file: %w", err)
	}
	_, err = f.WriteString(END)
	if err != nil {
		return fmt.Errorf("failed to write END server-info file: %w", err)
	}
	return nil
}
