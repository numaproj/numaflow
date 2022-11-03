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
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/window"
)

// Options for forwarding the message
type Options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// windowOpts Options for window
	windowOpts *window.Options
}

type Option func(*Options) error

func DefaultOptions() *Options {
	return &Options{
		readBatchSize: dfv1.DefaultReadBatchSize,
		windowOpts:    window.DefaultOptions(),
	}
}

// WithReadBatchSize sets the read batch size
func WithReadBatchSize(f int64) Option {
	return func(o *Options) error {
		o.readBatchSize = f
		return nil
	}
}

// WithWindowOptions sets different window options
func WithWindowOptions(opts ...window.Option) Option {
	return func(options *Options) error {
		for _, opt := range opts {
			err := opt(options.windowOpts)
			if err != nil {
				return err
			}
		}
		return nil
	}
}
