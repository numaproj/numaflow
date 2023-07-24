package nats

// Options for NATS client pool
type Options struct {
	// ClientPoolSize is the size of the NATS client pool
	clientPoolSize int
}

func defaultOptions() *Options {
	return &Options{
		clientPoolSize: 3,
	}
}

// Option is a function on the options for a NATS client pool
type Option func(*Options)

// WithClientPoolSize sets the size of the NATS client pool
func WithClientPoolSize(size int) Option {
	return func(o *Options) {
		o.clientPoolSize = size
	}
}
