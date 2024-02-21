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

package pnf

import (
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned"
	"github.com/numaproj/numaflow/pkg/window"
)

type options struct {
	gcEventsTracker unaligned.GCEventsWAL
	windowType      window.Type
}

type Option func(options *options) error

// WithGCEventsTracker sets the GCEventsWAL option
func WithGCEventsTracker(gcTracker unaligned.GCEventsWAL) Option {
	return func(o *options) error {
		o.gcEventsTracker = gcTracker
		return nil
	}
}

// WithWindowType sets the window type option
func WithWindowType(windowType window.Type) Option {
	return func(o *options) error {
		o.windowType = windowType
		return nil
	}
}
