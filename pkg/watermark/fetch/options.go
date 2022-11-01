package fetch

type processorManagerOptions struct {
	// podHeartbeatRate uses second as time unit
	podHeartbeatRate int64
	// refreshingProcessorsRate uses second as time unit
	refreshingProcessorsRate int64
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
