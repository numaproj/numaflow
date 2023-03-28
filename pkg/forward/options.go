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
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// options for forwarding the message
type options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// udfConcurrency sets the concurrency for concurrent UDF processing
	udfConcurrency int
	// retryInterval is the time.Duration to sleep before retrying
	retryInterval time.Duration
	// vertexType indicates the type of the vertex
	vertexType dfv1.VertexType
	// srcWatermarkPublisher is used to publish source watermark
	srcWatermarkPublisher isb.SourceWatermarkPublisher
	// logger is used to pass the logger variable
	logger *zap.SugaredLogger
}

type Option func(*options) error

func DefaultOptions() *options {
	return &options{
		readBatchSize:  dfv1.DefaultReadBatchSize,
		udfConcurrency: dfv1.DefaultReadBatchSize,
		retryInterval:  time.Millisecond,
		logger:         logging.NewLogger(),
	}
}

// WithRetryInterval sets the retry interval
func WithRetryInterval(f time.Duration) Option {
	return func(o *options) error {
		o.retryInterval = time.Duration(f)
		return nil
	}
}

// WithReadBatchSize sets the read batch size
func WithReadBatchSize(f int64) Option {
	return func(o *options) error {
		o.readBatchSize = f
		return nil
	}
}

// WithUDFConcurrency ste concurrency for UDF processing
func WithUDFConcurrency(f int) Option {
	return func(o *options) error {
		o.udfConcurrency = f
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

// WithVertexType sets the type of the vertex
func WithVertexType(t dfv1.VertexType) Option {
	return func(o *options) error {
		o.vertexType = t
		return nil
	}
}

// WithSourceWatermarkPublisher sets the source watermark publisher
func WithSourceWatermarkPublisher(p isb.SourceWatermarkPublisher) Option {
	return func(o *options) error {
		o.srcWatermarkPublisher = p
		return nil
	}
}
