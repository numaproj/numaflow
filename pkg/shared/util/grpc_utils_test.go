package util

import (
	"testing"

	"github.com/numaproj/numaflow-go/pkg/info"
	"github.com/stretchr/testify/assert"
)

var testVersionMappingConfig = map[string]sdkConstraints{
	"1.1.7": {
		info.Go:     "0.5.0-0",
		info.Python: "0.5.0a",
		info.Java:   "0.5.0-0",
	},
	"1.1.8": {
		info.Go:     "0.6.0-0",
		info.Python: "0.6.0a",
		info.Java:   "0.6.0-0",
	},
}

func TestCheckCompatibility(t *testing.T) {
	tests := []struct {
		name                 string
		serverInfo           *info.ServerInfo
		versionMappingConfig map[string]sdkConstraints
		numaflowVersion      string
		shouldErr            bool
		errMessage           string
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
			versionMappingConfig: testVersionMappingConfig,
			numaflowVersion:      "1.1.8",
			shouldErr:            true,
			errMessage:           "SDK version 0.5.3 must be upgraded to >= 0.6.0a or later, in order to work with current numaflow version 1.1.8",
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
			versionMappingConfig: testVersionMappingConfig,
			numaflowVersion:      "1.1.7",
			shouldErr:            true,
			errMessage:           "numaflow version 1.1.7 must be upgraded to >= 1.1.8-0 or later, in order to work with current SDK version 0.7.0-rc1",
		},
		{
			name: "Test with compatible numaflow and SDK version",
			serverInfo: &info.ServerInfo{
				Protocol:               info.UDS,
				Language:               info.Go,
				MinimumNumaflowVersion: "1.1.7-0",
				Version:                "0.5.0-rc2",
				Metadata:               nil,
			},
			versionMappingConfig: testVersionMappingConfig,
			numaflowVersion:      "1.1.7",
			shouldErr:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkCompatibility(tt.serverInfo, tt.versionMappingConfig, tt.numaflowVersion)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
				assert.Contains(t, err.Error(), tt.errMessage)
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}
