package client

import "time"

type options struct {
	tcpSockAddr                string
	udsSockAddr                string
	maxMessageSize             int
	serverInfoFilePath         string
	serverInfoReadinessTimeout time.Duration
}

// Option is the interface to apply options.
type Option func(*options)

// WithUdsSockAddr start the client with the given UDS sock addr. This is mainly used for testing purpose.
func WithUdsSockAddr(addr string) Option {
	return func(opts *options) {
		opts.udsSockAddr = addr
	}
}

// WithTcpSockAddr start the client with the given TCP sock addr. This is mainly used for testing purpose.
func WithTcpSockAddr(addr string) Option {
	return func(opts *options) {
		opts.tcpSockAddr = addr
	}
}

// WithMaxMessageSize sets the server max receive message size and the server max send message size to the given size.
func WithMaxMessageSize(size int) Option {
	return func(opts *options) {
		opts.maxMessageSize = size
	}
}

// WithServerInfoFilePath sets the server info file path to the given path.
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
