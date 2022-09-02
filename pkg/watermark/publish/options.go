package publish

import (
	"time"

	"github.com/nats-io/nats.go"
)

type publishOptions struct {
	autoRefreshHeartbeat bool
	bucketConfigs        *nats.KeyValueConfig
	podHeartbeatRate     int64
	// Watermark delay.
	// It should only be used in a source publisher.
	delay time.Duration
}

type PublishOption func(*publishOptions)

func WithAutoRefreshHeartbeatDisabled() PublishOption {
	return func(opts *publishOptions) {
		opts.autoRefreshHeartbeat = false
	}
}

func WithBucketConfigs(cfgs *nats.KeyValueConfig) PublishOption {
	return func(opts *publishOptions) {
		opts.bucketConfigs = cfgs
	}
}

func WithPodHeartbeatRate(rate int64) PublishOption {
	return func(opts *publishOptions) {
		opts.podHeartbeatRate = rate
	}
}

// WithDelay sets the watermark delay
func WithDelay(t time.Duration) PublishOption {
	return func(opts *publishOptions) {
		opts.delay = t
	}
}
