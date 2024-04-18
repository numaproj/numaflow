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

func TestCheckNumaflowCompatibility(t *testing.T) {
	tests := []struct {
		name               string
		numaflowVersion    string
		minNumaflowVersion string
		shouldErr          bool
		errMessage         string
	}{
		{
			name:               "Test with incompatible numaflow version",
			numaflowVersion:    "v1.1.6",
			minNumaflowVersion: "1.1.7",
			shouldErr:          true,
			errMessage:         "numaflow version 1.1.6 must be upgraded to at least 1.1.7, in order to work with current SDK version",
		},
		{
			name:               "Test with empty MinimumNumaflowVersion field",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "",
			shouldErr:          true,
			errMessage:         "server info does not contain minimum numaflow version. Upgrade SDK to latest version",
		},
		{
			name:               "Test with compatible numaflow version",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "1.1.6",
			shouldErr:          false,
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

func TestCheckSDKCompatibility(t *testing.T) {
	tests := []struct {
		name                        string
		sdkVersion                  string
		sdkLanguage                 info.Language
		minimumSupportedSDKVersions sdkConstraints
		shouldErr                   bool
		errMessage                  string
	}{
		{
			name:                        "Test with incompatible SDK version",
			sdkVersion:                  "v0.5.3-rc2",
			sdkLanguage:                 info.Go,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.5.3-rc2 must be upgraded to at least 0.6.0-0, in order to work with current numaflow version",
		},
		{
			name:                        "Test with compatible SDK version",
			sdkVersion:                  "v0.6.0a2",
			sdkLanguage:                 info.Python,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
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