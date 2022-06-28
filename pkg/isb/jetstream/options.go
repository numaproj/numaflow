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
	// useWriteInfoAsRate indicates whether to check the write sequence for rate calculation
	useWriteInfoAsRate bool
	// rateLookbackSeconds is the look back seconds for rate calculation
	rateLookbackSeconds int64
}

func defaultWriteOptions() *writeOptions {
	return &writeOptions{
		maxLength:           dfv1.DefaultBufferLength,
		bufferUsageLimit:    dfv1.DefaultBufferUsageLimit,
		refreshInterval:     1 * time.Second,
		useWriteInfoAsRate:  false,
		rateLookbackSeconds: 180,
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

// WithUsingWriteInfoAsRate sets whether to check sequence for rate calculation
func WithUsingWriteInfoAsRate(yes bool) WriteOption {
	return func(o *writeOptions) error {
		o.useWriteInfoAsRate = yes
		return nil
	}
}

func WithWriteRateLookbackSeconds(seconds int64) WriteOption {
	return func(o *writeOptions) error {
		o.rateLookbackSeconds = seconds
		return nil
	}
}

// options for reading from JetStream
type readOptions struct {
	// readTimeOut is the timeout needed for read timeout
	readTimeOut time.Duration
	// Whether to run ack information check
	useAckInfoAsRate bool
	// ackCheckInterval is the interval for stream information check such as pending messages
	ackInfoCheckInterval time.Duration
	// rateLookbackSeconds is the look back seconds for rate calculation
	rateLookbackSeconds int64
}

type ReadOption func(*readOptions) error

// WithReadTimeOut is used to set read timeout option
func WithReadTimeOut(timeout time.Duration) ReadOption {
	return func(o *readOptions) error {
		o.readTimeOut = timeout
		return nil
	}
}

// WithUsingAckInfoAsRate is used to set whether to run ack information check in the reader, which is for ack rate calculation
func WithUsingAckInfoAsRate(yes bool) ReadOption {
	return func(o *readOptions) error {
		o.useAckInfoAsRate = yes
		return nil
	}
}

// WithAckInfoCheckInterval is used to set the periodical ack information check interval
func WithAckInfoCheckInterval(t time.Duration) ReadOption {
	return func(o *readOptions) error {
		o.ackInfoCheckInterval = t
		return nil
	}
}

func WithAckRateLookbackSeconds(seconds int64) ReadOption {
	return func(o *readOptions) error {
		o.rateLookbackSeconds = seconds
		return nil
	}
}

func defaultReadOptions() *readOptions {
	return &readOptions{
		readTimeOut:          time.Second,
		useAckInfoAsRate:     false,
		ackInfoCheckInterval: 3 * time.Second,
		rateLookbackSeconds:  180,
	}
}
