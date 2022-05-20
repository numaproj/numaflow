package isbsvc

import (
	"context"
)

// ISBService is an interface used to do the operations on ISBS
type ISBService interface {
	CreateBuffers(ctx context.Context, buffers []string, opts ...BufferCreateOption) error
	DeleteBuffers(ctx context.Context, buffers []string) error
	ValidateBuffers(ctx context.Context, buffers []string) error
	GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error)
}

// bufferCreateOptions describes the options for creating buffers
type bufferCreateOptions struct {
	// bufferConfig is configuratiion for the to be created buffer
	bufferConfig string
}

type BufferCreateOption func(*bufferCreateOptions) error

// WithBufferConfig sets buffer config option
func WithBufferConfig(conf string) BufferCreateOption {
	return func(o *bufferCreateOptions) error {
		o.bufferConfig = conf
		return nil
	}
}

// BufferInfo wraps the buffer state information
type BufferInfo struct {
	Name            string
	PendingCount    int64
	AckPendingCount int64
	TotalMessages   int64
}
