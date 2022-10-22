package fetch

type processorManagerOptions struct {
	// podHeartbeatRate uses second as time unit
	podHeartbeatRate int64
	// refreshingProcessorsRate uses second as time unit
	refreshingProcessorsRate int64
	separateOTBucket         bool
}

// ProcessorManagerOption set options for FromVertex.
type ProcessorManagerOption func(*processorManagerOptions)

// WithPodHeartbeatRate sets the heartbeat rate in seconds.
func WithPodHeartbeatRate(rate int64) ProcessorManagerOption {
	return func(opts *processorManagerOptions) {
		opts.podHeartbeatRate = rate
	}
}

// WithRefreshingProcessorsRate sets the processor refreshing rate in seconds.
func WithRefreshingProcessorsRate(rate int64) ProcessorManagerOption {
	return func(opts *processorManagerOptions) {
		opts.refreshingProcessorsRate = rate
	}
}

// WithSeparateOTBuckets creates a different bucket for maintaining each processor offset-timeline.
func WithSeparateOTBuckets(separate bool) ProcessorManagerOption {
	return func(opts *processorManagerOptions) {
		opts.separateOTBucket = separate
	}
}
