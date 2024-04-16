package util

import (
	"testing"

	"github.com/numaproj/numaflow-go/pkg/info"
	"github.com/stretchr/testify/assert"
)

func TestIsCompatible(t *testing.T) {
	tests := []struct {
		name       string
		serverInfo *info.ServerInfo
		shouldErr  bool
	}{
		{
			name: "Test with incompatible version",
			serverInfo: &info.ServerInfo{
				Protocol: info.UDS,
				Language: info.Python,
				Version:  "0.6.0",
				Metadata: nil,
			},
			shouldErr: true,
		},
		{
			name: "Test with compatible version",
			serverInfo: &info.ServerInfo{
				Protocol: info.UDS,
				Language: info.Go,
				Version:  "0.7.0-rc1",
				Metadata: nil,
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkCompatibility(tt.serverInfo)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}
