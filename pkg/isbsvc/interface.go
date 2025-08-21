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
	"fmt"
)

// ISBService is an interface used to do the operations on ISBSvc
type ISBService interface {
	// CreateBuffersAndBuckets creates buffers and buckets
	CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string, opts ...CreateOption) error
	// DeleteBuffersAndBuckets deletes buffers and buckets
	DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string) error
	// ValidateBuffersAndBuckets validates buffers and buckets
	ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string) error
	// GetBufferInfo returns buffer info for the given buffer
	GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error)
}

// createOptions describes the options for creating buffers and buckets
type createOptions struct {
	// config is configuration for the to be created buffers and buckets
	config string
}

type CreateOption func(*createOptions) error

// WithConfig sets buffer and bucket config option
func WithConfig(conf string) CreateOption {
	return func(o *createOptions) error {
		o.config = conf
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

func JetStreamProcessorKVName(bucketName string) string {
	return fmt.Sprintf("%s_PROCESSORS", bucketName)
}

func JetStreamOTKVName(bucketName string) string {
	return fmt.Sprintf("%s_OT", bucketName)
}
