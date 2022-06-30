package util

import (
	"os"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// IsWatermarkEnabled returns true of watermark has been enabled.
func IsWatermarkEnabled() bool {
	if val, ok := os.LookupEnv(dfv1.EnvWatermarkOn); ok && val == "true" {
		return true
	}
	return false
}
