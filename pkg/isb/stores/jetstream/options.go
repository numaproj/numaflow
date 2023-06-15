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

package jetstream

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// options for writing to JetStream
type writeOptions struct {
	// maxLength is the maximum length of the stream before it reaches full
	maxLength int64
	// partitionUsageLimit is the limit of partition usage before we declare it as full
	partitionUsageLimit float64
	// refreshInterval is used to provide the default refresh interval
	refreshInterval time.Duration
	// partitionFullWritingStrategy is the writing strategy when partition is full
	partitionFullWritingStrategy dfv1.BufferFullWritingStrategy
}

func defaultWriteOptions() *writeOptions {
	return &writeOptions{
		maxLength:                    dfv1.DefaultPartitionLength,
		partitionUsageLimit:          dfv1.DefaultPartitionUsageLimit,
		refreshInterval:              1 * time.Second,
		partitionFullWritingStrategy: dfv1.RetryUntilSuccess,
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

// WithPartitionUsageLimit sets partition usage limit option
func WithPartitionUsageLimit(usageLimit float64) WriteOption {
	return func(o *writeOptions) error {
		o.partitionUsageLimit = usageLimit
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

// WithPartitionFullWritingStrategy sets the writing strategy when partition is full
func WithPartitionFullWritingStrategy(s dfv1.BufferFullWritingStrategy) WriteOption {
	return func(o *writeOptions) error {
		o.partitionFullWritingStrategy = s
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
