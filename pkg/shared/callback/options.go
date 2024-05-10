package callback

import "time"

// Options holds optional parameters for the Publisher.
type Options struct {
	// HTTPTimeout specifies the timeout for HTTP requests.
	HTTPTimeout time.Duration
	// LRUCacheSize specifies the size of the LRU cache for HTTP clients.
	LRUCacheSize int
}

// DefaultOptions returns the default options.
func DefaultOptions() *Options {
	return &Options{
		HTTPTimeout:  10 * time.Second,
		LRUCacheSize: 50,
	}
}

// OptionFunc is a function that applies an option to the Publisher.
type OptionFunc func(*Options)

// WithHTTPTimeout sets the HTTP timeout.
func WithHTTPTimeout(timeout time.Duration) OptionFunc {
	return func(o *Options) {
		o.HTTPTimeout = timeout
	}
}

// WithLRUCacheSize sets the LRU cache size.
func WithLRUCacheSize(size int) OptionFunc {
	return func(o *Options) {
		o.LRUCacheSize = size
	}
}
