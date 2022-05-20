package jetstream

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// options for writing to JetStream
type writeOptions struct {
	// maxLength is the maximum length of the stream before it reaches full
	maxLength int64
	// bufferUsageLimit is the limit of buffer usage before we declare it as full
	bufferUsageLimit float64
	// refreshInterval is used to provide the default refresh interval
	refreshInterval time.Duration
}

func defaultWriteOptions() *writeOptions {
	return &writeOptions{
		maxLength:        dfv1.DefaultBufferLength,
		bufferUsageLimit: dfv1.DefaultBufferUsageLimit,
		refreshInterval:  1 * time.Second,
	}
}

type WriteOption func(*writeOptions) error

// WithMaxLength sets buffer max length option
func WithMaxLength(length int64) WriteOption {
	return func(o *writeOptions) error {
		o.maxLength = length
		return nil
	}
}

// WithBufferUsageLimit sets buffer usage limit option
func WithBufferUsageLimit(usageLimit float64) WriteOption {
	return func(o *writeOptions) error {
		o.bufferUsageLimit = usageLimit
		return nil
	}
}

// WithRefreshInterval sets refresh interval option
func WithRefreshInterval(refreshInterval time.Duration) WriteOption {
	return func(o *writeOptions) error {
		o.refreshInterval = refreshInterval
		return nil
	}
}

// options for reading from JetStream
type readOptions struct {
	// readTimeOut is the timeout needed for read timeout
	readTimeOut time.Duration
}

type ReadOption func(*readOptions) error

// WithReadTimeOut is used to set read timeout option
func WithReadTimeOut(timeout time.Duration) ReadOption {
	return func(o *readOptions) error {
		o.readTimeOut = timeout
		return nil
	}
}
func defaultReadOptions() *readOptions {
	return &readOptions{
		readTimeOut: time.Second,
	}
}
