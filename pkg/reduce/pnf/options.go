package pnf

// options for forwarding the message
type options struct {
	// toVertexPartitions is the number of partitions for the to vertex
	toVertexPartitions int32
}

type Option func(*options) error

func DefaultOptions() *options {
	return &options{
		toVertexPartitions: 1,
	}
}

// WithToVertexPartitions sets the number of partitions for the to vertex
func WithToVertexPartitions(f int32) Option {
	return func(o *options) error {
		o.toVertexPartitions = f
		return nil
	}
}
