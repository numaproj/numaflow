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

package sdkclient

type Options struct {
	udsSockAddr    string
	maxMessageSize int
}

// UdsSockAddr returns the UDS sock addr.
func (o *Options) UdsSockAddr() string {
	return o.udsSockAddr
}

// MaxMessageSize returns the max message size.
func (o *Options) MaxMessageSize() int {
	return o.maxMessageSize
}

// DefaultOptions returns the default options.
func DefaultOptions(address string) *Options {
	return &Options{
		maxMessageSize: DefaultGRPCMaxMessageSize,
		udsSockAddr:    address,
	}
}

// Option is the interface to apply Options.
type Option func(*Options)

// WithUdsSockAddr start the client with the given UDS sock addr. This is mainly used for testing purpose.
func WithUdsSockAddr(addr string) Option {
	return func(opts *Options) {
		opts.udsSockAddr = addr
	}
}

// WithMaxMessageSize sets the server max receive message size and the server max send message size to the given size.
func WithMaxMessageSize(size int) Option {
	return func(opts *Options) {
		opts.maxMessageSize = size
	}
}
