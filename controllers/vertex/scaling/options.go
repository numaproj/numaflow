package scaling

type options struct {
	// Number of workers working on auto scaling
	workers int
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		workers: 20,
	}
}

func WithWorkers(n int) Option {
	return func(o *options) {
		o.workers = n
	}
}
