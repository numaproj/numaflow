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

package reduce

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// Options for forwarding the message
type Options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// allowedLateness is the time.Duration it waits after the watermark has progressed for late-date to be included
	allowedLateness time.Duration
}

type Option func(*Options) error

func DefaultOptions() *Options {
	return &Options{
		readBatchSize:   dfv1.DefaultReadBatchSize,
		allowedLateness: time.Duration(0),
	}
}

// WithReadBatchSize sets the read batch size
func WithReadBatchSize(f int64) Option {
	return func(o *Options) error {
		o.readBatchSize = f
		return nil
	}
}

// WithAllowedLateness sets allowedLateness
func WithAllowedLateness(t time.Duration) Option {
	return func(o *Options) error {
		o.allowedLateness = t
		return nil
	}
}
