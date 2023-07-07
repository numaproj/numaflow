package client

import "time"

type options struct {
	sockAddr                   string
	serverInfoFilePath         string
	serverInfoReadinessTimeout time.Duration
	maxMessageSize             int
}

// Option is the interface to apply options.
type Option func(*options)

// WithSockAddr start the client with the given sock addr. This is mainly used for testing purpose.
func WithSockAddr(addr string) Option {
	return func(opts *options) {
		opts.sockAddr = addr
	}
}

// WithServerInfoFilePath start the client with the given server info file path. This is mainly used for testing purpose.
func WithServerInfoFilePath(f string) Option {
	return func(o *options) {
		o.serverInfoFilePath = f
	}
}

// WithServerInfoReadinessTimeout sets the server info readiness timeout to the given timeout.
func WithServerInfoReadinessTimeout(t time.Duration) Option {
	return func(o *options) {
		o.serverInfoReadinessTimeout = t
	}
}

// WithMaxMessageSize sets the max message size to the given size.
func WithMaxMessageSize(size int) Option {
	return func(o *options) {
		o.maxMessageSize = size
	}
}
