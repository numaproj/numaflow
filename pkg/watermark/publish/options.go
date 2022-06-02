package publish

import "github.com/nats-io/nats.go"

type publishOptions struct {
	autoRefreshHeartbeat bool
	bucketConfigs        *nats.KeyValueConfig
	podHeartbeatRate     int64
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
