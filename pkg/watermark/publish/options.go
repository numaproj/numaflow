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

package publish

import (
	"time"
)

type publishOptions struct {
	// autoRefreshHeartbeat indicates whether to auto refresh heartbeat
	autoRefreshHeartbeat bool
	// The interval of refresh heartbeat
	podHeartbeatRate int64
	// Watermark delay.
	// It should only be used in a source publisher.
	delay time.Duration
	// Whether it is source publisher or not
	// isSource and isSink should not be both true
	isSource bool
	// Whether it is sink publisher or not
	// isSource and isSink should not be both true
	isSink bool
}

type PublishOption func(*publishOptions)

func WithAutoRefreshHeartbeatDisabled() PublishOption {
	return func(opts *publishOptions) {
		opts.autoRefreshHeartbeat = false
	}
}

func WithPodHeartbeatRate(rate int64) PublishOption {
	return func(opts *publishOptions) {
		opts.podHeartbeatRate = rate
	}
}

// WithDelay sets the watermark delay
func WithDelay(t time.Duration) PublishOption {
	return func(opts *publishOptions) {
		opts.delay = t
	}
}

// IsSource indicates it's a source publisher
func IsSource() PublishOption {
	return func(opts *publishOptions) {
		opts.isSource = true
	}
}

// IsSink indicates it's a sink publisher
func IsSink() PublishOption {
	return func(opts *publishOptions) {
		opts.isSink = true
	}
}
