/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
