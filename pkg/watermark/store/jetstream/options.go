package jetstream

import "time"

// options for KV watcher.
type options struct {
	// watcherCreationThreshold is the threshold after which we will check if the watcher is still working.
	// if the store is getting updates but the watcher is not, we will re-create the watcher.
	watcherCreationThreshold time.Duration
}

func defaultOptions() *options {
	return &options{
		watcherCreationThreshold: 120 * time.Second,
	}
}

// Option is a function on the options kv watcher
type Option func(*options)

// WithWatcherCreationThreshold sets the watcherCreationThreshold
func WithWatcherCreationThreshold(d time.Duration) Option {
	return func(o *options) {
		o.watcherCreationThreshold = d
	}
}
