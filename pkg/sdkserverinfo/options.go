package sdkserverinfo

import "time"

type Options struct {
	serverInfoFilePath         string
	serverInfoReadinessTimeout time.Duration
}

// ServerInfoFilePath returns the server info file path.
func (o *Options) ServerInfoFilePath() string {
	return o.serverInfoFilePath
}

// ServerInfoReadinessTimeout returns the server info readiness timeout.
func (o *Options) ServerInfoReadinessTimeout() time.Duration {
	return o.serverInfoReadinessTimeout
}

// DefaultOptions returns the default options.
func DefaultOptions() *Options {
	return &Options{
		serverInfoReadinessTimeout: 30 * time.Second, // Default timeout is 120 seconds
	}
}

// Option is the interface to apply Options.
type Option func(*Options)

// WithServerInfoFilePath sets the server info file path to the given path.
func WithServerInfoFilePath(f string) Option {
	return func(o *Options) {
		o.serverInfoFilePath = f
	}
}

// WithServerInfoReadinessTimeout sets the server info readiness timeout to the given timeout.
func WithServerInfoReadinessTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.serverInfoReadinessTimeout = t
	}
}
