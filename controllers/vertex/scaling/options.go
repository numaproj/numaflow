package scaling

type options struct {
	// Number of workers working on auto scaling
	workers int
	// Time in milliseconds, each element in the work queue will be picked up in an interval of this period of time.
	taskInterval int
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		workers:      20,
		taskInterval: 30000,
	}
}

func WithWorkers(n int) Option {
	return func(o *options) {
		o.workers = n
	}
}
