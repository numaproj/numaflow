package simplebuffer

import "time"

// Options for simple buffer
type options struct {
	// readTimeOut is the timeout needed for read timeout
	readTimeOut time.Duration
}

type Option func(options *options) error

// WithReadTimeOut is used to set read timeout option
func WithReadTimeOut(timeout time.Duration) Option {
	return func(o *options) error {
		o.readTimeOut = timeout
		return nil
	}
}
