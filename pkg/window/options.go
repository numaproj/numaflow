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

package window

import (
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"time"
)

type Options struct {
	// windowType to specify the window type(fixed, sliding or session)
	windowType dfv1.WindowType
	// windowDuration to specify the duration of the window
	windowDuration time.Duration
}

func DefaultOptions() *Options {
	return &Options{
		windowType:     dfv1.DefaultWindowType,
		windowDuration: dfv1.DefaultWindowDuration,
	}
}

type Option func(options *Options) error

// WithWindowType sets the window type
func WithWindowType(wt dfv1.WindowType) Option {
	return func(o *Options) error {
		o.windowType = wt
		return nil
	}
}

// WithWindowDuration sets the window duration
func WithWindowDuration(wd time.Duration) Option {
	return func(o *Options) error {
		o.windowDuration = wd
		return nil
	}
}
