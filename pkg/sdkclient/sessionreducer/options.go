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

package sessionreducer

import "time"

type options struct {
	tcpSockAddr                string
	udsSockAddr                string
	maxMessageSize             int
	serverInfoFilePath         string
	serverInfoReadinessTimeout time.Duration
}

// Option is the interface to apply options.
type Option func(*options)

// WithUdsSockAddr start the client with the given UDS sock addr. This is mainly used for testing purpose.
func WithUdsSockAddr(addr string) Option {
	return func(opts *options) {
		opts.udsSockAddr = addr
	}
}

// WithTcpSockAddr start the client with the given TCP sock addr. This is mainly used for testing purpose.
func WithTcpSockAddr(addr string) Option {
	return func(opts *options) {
		opts.tcpSockAddr = addr
	}
}

// WithMaxMessageSize sets the server max receive message size and the server max send message size to the given size.
func WithMaxMessageSize(size int) Option {
	return func(opts *options) {
		opts.maxMessageSize = size
	}
}

// WithServerInfoFilePath sets the server info file path to the given path.
func WithServerInfoFilePath(f string) Option {
	return func(o *options) {
		o.serverInfoFilePath = f
	}
}

// WithServerInfoReadinessTimeout sets the server info readiness timeout to the given timeout.
func WithServerInfoReadinessTimeout(t time.Duration) Option {
	return func(o *options) {
		o.serverInfoReadinessTimeout = t
	}
}
