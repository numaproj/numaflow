package publish

import (
	"time"
)

type publishOptions struct {
	// autoRefreshHeartbeat indicates whether to auto refresh heartbeat
	autoRefreshHeartbeat bool
	// The interval of refresh heartbeat
	podHeartbeatRate int64
	// Watermark delay.
	// It should only be used in a source publisher.
	delay time.Duration
	// Whether it is source publisher or not
	isSource bool
}

type PublishOption func(*publishOptions)

func WithAutoRefreshHeartbeatDisabled() PublishOption {
	return func(opts *publishOptions) {
		opts.autoRefreshHeartbeat = false
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

// IsSource indicates it's a source publisher
func IsSource() PublishOption {
	return func(opts *publishOptions) {
		opts.isSource = true
	}
}
