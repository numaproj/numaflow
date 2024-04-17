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

package forward

import (
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sinks/sinker"
)

// options for forwarding the message
type options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// sinkConcurrency sets the concurrency for concurrent processing
	sinkConcurrency int
	// retryInterval is the time.Duration to sleep before retrying
	retryInterval time.Duration
	// fbSinkWriter is the writer for the fallback sink
	fbSinkWriter sinker.SinkWriter
	// logger is used to pass the logger variable
	logger *zap.SugaredLogger
}

type Option func(*options) error

func DefaultOptions() *options {
	return &options{
		readBatchSize:   dfv1.DefaultReadBatchSize,
		sinkConcurrency: dfv1.DefaultReadBatchSize,
		retryInterval:   time.Millisecond,
		logger:          logging.NewLogger(),
	}
}

// WithReadBatchSize sets the read batch size
func WithReadBatchSize(f int64) Option {
	return func(o *options) error {
		o.readBatchSize = f
		return nil
	}
}

// WithSinkConcurrency sets concurrency for processing
func WithSinkConcurrency(f int) Option {
	return func(o *options) error {
		o.sinkConcurrency = f
		return nil
	}
}

// WithRetryInterval sets the retry interval
func WithRetryInterval(f time.Duration) Option {
	return func(o *options) error {
		o.retryInterval = time.Duration(f)
		return nil
	}
}

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *options) error {
		o.logger = l
		return nil
	}
}

// WithFbSinkWriter sets the fallback sink writer
func WithFbSinkWriter(sinkWriter sinker.SinkWriter) Option {
	return func(o *options) error {
		o.fbSinkWriter = sinkWriter
		return nil
	}
}
