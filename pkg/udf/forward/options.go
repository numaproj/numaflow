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
	"fmt"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/callback"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/forward/applier"
)

// options for forwarding the message
type options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// udfConcurrency sets the concurrency for concurrent map UDF processing
	udfConcurrency int
	// retryInterval is the time.Duration to sleep before retrying
	retryInterval time.Duration
	// logger is used to pass the logger variable
	logger *zap.SugaredLogger
	// cbPublisher is the callback publisher for the vertex.
	cbPublisher *callback.Uploader
	// unaryMapUdfApplier is the UDF applier for a unary map mode
	unaryMapUdfApplier applier.MapApplier
	// streamMapUdfApplier is the UDF applier for a server streaming map mode
	streamMapUdfApplier applier.MapStreamApplier
	// batchMapUdfApplier is the UDF applier for a batch map mode
	batchMapUdfApplier applier.BatchMapApplier
}

type Option func(*options) error

func DefaultOptions() *options {
	return &options{
		readBatchSize:       dfv1.DefaultReadBatchSize,
		udfConcurrency:      dfv1.DefaultReadBatchSize,
		retryInterval:       time.Millisecond,
		logger:              logging.NewLogger(),
		unaryMapUdfApplier:  nil,
		batchMapUdfApplier:  nil,
		streamMapUdfApplier: nil,
	}
}

// WithRetryInterval sets the retry interval
func WithRetryInterval(f time.Duration) Option {
	return func(o *options) error {
		o.retryInterval = f
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

// WithUDFConcurrency sets concurrency for map UDF processing
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

// WithCallbackUploader sets the callback uploader for the vertex
func WithCallbackUploader(cp *callback.Uploader) Option {
	return func(o *options) error {
		o.cbPublisher = cp
		return nil
	}
}

// Options to set the map mode to be used, as all of them are mutually exclusive, at one point of time
// one of them can be enabled, and others are set to nil
// NOTE: if multiple of these are passed with valid appliers, we consider that as an invalid scenario
// There 3 are mutually exclusive by virtue of the fields that they control, and thus only one of them should be used.
// Currently, cannot think of a valid case where multiple of them might be required to be passed together, if later
// on we feel the need, we can allow that and check for mutual exclusivity later on.
//
// Overwrites are allowed for the same applier, and hence a single With call can be added multiple times with different
// values of the same applier.
// exactly one of the appliers should not be nil as only one mode can be active at a time, not more not less only 1
// There is a case where none of them is set which cannot be allowed

// WithUDFBatchMap enables the batch map for UDF if provided with a non-nil applier
func WithUDFBatchMap(f applier.BatchMapApplier) Option {
	return func(o *options) error {
		// only overwrite for the same option is allowed, other two cannot be set
		if f != nil && o.unaryMapUdfApplier == nil && o.streamMapUdfApplier == nil {
			o.batchMapUdfApplier = f
			return nil
		}
		return fmt.Errorf("invalid option")
	}
}

// WithUDFUnaryMap enables the unary map for UDF if provided with a non-nil applier
func WithUDFUnaryMap(f applier.MapApplier) Option {
	return func(o *options) error {
		// only overwrite for the same option is allowed, other two cannot be set
		if f != nil && o.batchMapUdfApplier == nil && o.streamMapUdfApplier == nil {
			o.unaryMapUdfApplier = f
			return nil
		}
		return fmt.Errorf("invalid option")
	}
}

// WithUDFStreamingMap sets streaming for map UDF processing if provided with a non-nil applier
func WithUDFStreamingMap(f applier.MapStreamApplier) Option {
	return func(o *options) error {
		// only overwrite for the same option is allowed, other two cannot be set
		if f != nil && o.unaryMapUdfApplier == nil && o.batchMapUdfApplier == nil {
			o.streamMapUdfApplier = f
			return nil
		}
		return fmt.Errorf("invalid option")
	}
}
