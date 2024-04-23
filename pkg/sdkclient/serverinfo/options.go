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

package serverinfo

import "time"

type Options struct {
	serverInfoFilePath         string
	serverInfoReadinessTimeout time.Duration
}

// ServerInfoFilePath returns the server info file path.
func (o *Options) ServerInfoFilePath() string {
	return o.serverInfoFilePath
}

// ServerInfoReadinessTimeout returns the server info readiness timeout.
func (o *Options) ServerInfoReadinessTimeout() time.Duration {
	return o.serverInfoReadinessTimeout
}

// DefaultOptions returns the default options.
func DefaultOptions() *Options {
	return &Options{
		serverInfoReadinessTimeout: 120 * time.Second, // Default timeout is 120 seconds
	}
}

// Option is the interface to apply Options.
type Option func(*Options)

// WithServerInfoFilePath sets the server info file path to the given path.
func WithServerInfoFilePath(f string) Option {
	return func(o *Options) {
		o.serverInfoFilePath = f
	}
}

// WithServerInfoReadinessTimeout sets the server info readiness timeout to the given timeout.
func WithServerInfoReadinessTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.serverInfoReadinessTimeout = t
	}
}
