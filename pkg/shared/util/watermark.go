package util

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// IsWatermarkEnabled returns true of watermark has been enabled.
func IsWatermarkEnabled() bool {
	return LookupEnvStringOr(dfv1.EnvWatermarkDisabled, "false") != "true"
}

// GetWatermarkMaxDelay returns user configured watermark max delay
func GetWatermarkMaxDelay() time.Duration {
	// The ENV variable is populated by the controller from validated user input, should not be invalid format.
	d, _ := time.ParseDuration(LookupEnvStringOr(dfv1.EnvWatermarkMaxDelay, "0s"))
	return d
}
