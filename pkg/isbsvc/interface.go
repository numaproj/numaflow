package isbsvc

import (
	"context"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
)

// ISBService is an interface used to do the operations on ISBSvc
type ISBService interface {
	CreateBuffers(ctx context.Context, buffers []dfv1.Buffer, opts ...BufferCreateOption) error
	DeleteBuffers(ctx context.Context, buffers []dfv1.Buffer) error
	ValidateBuffers(ctx context.Context, buffers []dfv1.Buffer) error
	GetBufferInfo(ctx context.Context, buffer dfv1.Buffer) (*BufferInfo, error)
	CreateWatermarkFetcher(ctx context.Context, bufferName string) (fetch.Fetcher, error)
}

// bufferCreateOptions describes the options for creating buffers
type bufferCreateOptions struct {
	// bufferConfig is configuration for the to be created buffer
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
