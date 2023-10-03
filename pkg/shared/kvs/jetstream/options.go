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

import "time"

// options for KV watcher.
type options struct {
	// watcherCreationThreshold is the threshold after which we will check if the watcher is still working.
	// if the store is getting updates but the watcher is not, we will re-create the watcher.
	watcherCreationThreshold time.Duration
}

func defaultOptions() *options {
	return &options{
		watcherCreationThreshold: 120 * time.Second,
	}
}

// Option is a function on the options kv watcher
type Option func(*options)

// WithWatcherCreationThreshold sets the watcherCreationThreshold
func WithWatcherCreationThreshold(d time.Duration) Option {
	return func(o *options) {
		o.watcherCreationThreshold = d
	}
}
