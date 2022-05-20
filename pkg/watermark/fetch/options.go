package fetch

type vertexOptions struct {
	podHeartbeatRate         int64
	refreshingProcessorsRate int64
	separateOTBucket         bool
}

// VertexOption set options for FromVertex.
type VertexOption func(*vertexOptions)

// WithPodHeartbeatRate sets the heartbeat rate in seconds.
func WithPodHeartbeatRate(rate int64) VertexOption {
	return func(opts *vertexOptions) {
		opts.podHeartbeatRate = rate
	}
}

// WithRefreshingProcessorsRate sets the processor refreshing rate in seconds.
func WithRefreshingProcessorsRate(rate int64) VertexOption {
	return func(opts *vertexOptions) {
		opts.refreshingProcessorsRate = rate
	}
}

// WithSeparateOTBuckets creates a different bucket for maintaining each processor offset-timeline.
func WithSeparateOTBuckets(separate bool) VertexOption {
	return func(opts *vertexOptions) {
		opts.separateOTBucket = separate
	}
}
