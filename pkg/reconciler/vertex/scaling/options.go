package scaling

type options struct {
	// Number of workers working on autoscaling.
	workers int
	// Time in milliseconds, each element in the work queue will be picked up in an interval of this period of time.
	taskInterval int
	// Threshold of considering there's back pressure, a float value less than 1.
	backPressureThreshold float64
}

type Option func(*options)

func defaultOptions() *options {
	return &options{
		workers:               20,
		taskInterval:          30000,
		backPressureThreshold: 0.9,
	}
}

func WithWorkers(n int) Option {
	return func(o *options) {
		o.workers = n
	}
}

func WithTaskInterval(n int) Option {
	return func(o *options) {
		o.taskInterval = n
	}
}

func WithBackPressureThreshold(n float64) Option {
	return func(o *options) {
		o.backPressureThreshold = n
	}
}
