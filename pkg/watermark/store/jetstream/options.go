package jetstream

import "time"

// options for KV watcher.
type options struct {
	// heartbeatThreshold is the threshold after which the KV watcher will be created if we don't receive any heartbeat.
	heartbeatThreshold time.Duration
}

func defaultOptions() *options {
	return &options{
		heartbeatThreshold: 120 * time.Second,
	}
}

// Option is a function on the options kv watcher
type Option func(*options)

// WithHeartBeatThreshold sets the heartbeat threshold for the KV watcher.
func WithHeartBeatThreshold(d time.Duration) Option {
	return func(o *options) {
		o.heartbeatThreshold = d
	}
}
