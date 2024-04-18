package util

import (
	"testing"

	"github.com/numaproj/numaflow-go/pkg/info"
	"github.com/stretchr/testify/assert"
)

var testMinimumSupportedSDKVersions = sdkConstraints{
	info.Go:     "0.6.0-0",
	info.Python: "0.6.0a",
	info.Java:   "0.6.0-0",
}

func TestNumaflowCompatibility(t *testing.T) {
	tests := []struct {
		name               string
		numaflowVersion    string
		minNumaflowVersion string
		shouldErr          bool
		errMessage         string
	}{
		{
			name:               "Test with incompatible Numaflow version",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "1.1.5",
			shouldErr:          true,
			errMessage:         "SDK version 0.5.3 must be upgraded to at least 0.6.0a, in order to work with current numaflow version 1.1.7",
		},
		{
			name: "Test with incompatible numaflow version",
			serverInfo: &info.ServerInfo{
				Protocol:               info.UDS,
				Language:               info.Java,
				MinimumNumaflowVersion: "1.1.8-0",
				Version:                "0.7.0-rc1",
				Metadata:               nil,
			},
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			numaflowVersion:             "1.1.7",
			shouldErr:                   true,
			errMessage:                  "numaflow version 1.1.7 must be upgraded to at least 1.1.8-0, in order to work with current SDK version 0.7.0-rc1",
		},
		{
			name: "Test with compatible numaflow and SDK version",
			serverInfo: &info.ServerInfo{
				Protocol:               info.UDS,
				Language:               info.Go,
				MinimumNumaflowVersion: "1.1.7-0",
				Version:                "0.6.0-rc2",
				Metadata:               nil,
			},
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			numaflowVersion:             "1.1.7",
			shouldErr:                   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkNumaflowCompatibility(tt.serverInfo, tt.minimumSupportedSDKVersions, tt.numaflowVersion)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
				assert.Contains(t, err.Error(), tt.errMessage)
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}

func TestSDKCompatibility(t *testing.T) {
	tests := []struct {
		name                        string
		serverInfo                  *info.ServerInfo
		minimumSupportedSDKVersions sdkConstraints
		numaflowVersion             string
		shouldErr                   bool
		errMessage                  string
	}{
		{
			name: "Test with incompatible SDK version",
			serverInfo: &info.ServerInfo{
				Protocol:               info.UDS,
				Language:               info.Python,
				MinimumNumaflowVersion: "1.1.7-0",
				Version:                "0.5.3",
				Metadata:               nil,
			},
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			numaflowVersion:             "1.1.7",
			shouldErr:                   true,
			errMessage:                  "SDK version 0.5.3 must be upgraded to at least 0.6.0a, in order to work with current numaflow version 1.1.7",
		},
		{
			name: "Test with incompatible numaflow version",
			serverInfo: &info.ServerInfo{
				Protocol:               info.UDS,
				Language:               info.Java,
				MinimumNumaflowVersion: "1.1.8-0",
				Version:                "0.7.0-rc1",
				Metadata:               nil,
			},
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			numaflowVersion:             "1.1.7",
			shouldErr:                   true,
			errMessage:                  "numaflow version 1.1.7 must be upgraded to at least 1.1.8-0, in order to work with current SDK version 0.7.0-rc1",
		},
		{
			name: "Test with compatible numaflow and SDK version",
			serverInfo: &info.ServerInfo{
				Protocol:               info.UDS,
				Language:               info.Go,
				MinimumNumaflowVersion: "1.1.7-0",
				Version:                "0.6.0-rc2",
				Metadata:               nil,
			},
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			numaflowVersion:             "1.1.7",
			shouldErr:                   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSDKCompatibility(tt.serverInfo, tt.minimumSupportedSDKVersions, tt.numaflowVersion)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
				assert.Contains(t, err.Error(), tt.errMessage)
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}
