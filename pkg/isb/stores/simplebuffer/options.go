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

package simplebuffer

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// Options for simple buffer
type options struct {
	// readTimeOut is the timeout needed for read timeout
	readTimeOut time.Duration
	// onFullWritingStrategy is the writing strategy when buffer is full
	onFullWritingStrategy dfv1.OnFullWritingStrategy
}

type Option func(options *options) error

// WithReadTimeOut is used to set read timeout option
func WithReadTimeOut(timeout time.Duration) Option {
	return func(o *options) error {
		o.readTimeOut = timeout
		return nil
	}
}

// WithOnFullWritingStrategy sets the writing strategy when buffer is full
func WithOnFullWritingStrategy(s dfv1.OnFullWritingStrategy) Option {
	return func(o *options) error {
		o.onFullWritingStrategy = s
		return nil
	}
}
