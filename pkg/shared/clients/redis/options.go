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

package redis

import (
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// Options for writing to redis
type Options struct {
	// Pipelining enables redis pipeline
	Pipelining bool
	// InfoRefreshInterval refreshes the info at this interval
	InfoRefreshInterval time.Duration
	// LagDuration is the minimum permissable consumerLag that we consider as a valid consumerLag.
	LagDuration time.Duration
	// ReadTimeOut is the timeout needed for read timeout
	ReadTimeOut time.Duration
	// CheckBackLog is used to read all the PENDING entries from the stream
	CheckBackLog bool
	// MaxLength is the maximum length of the stream before it reaches full
	MaxLength int64
	// BufferUsageLimit is the limit of buffer usage before we declare it as full
	BufferUsageLimit float64
	// RefreshBufferWriteInfo is used to determine if we refresh buffer write info
	RefreshBufferWriteInfo bool
	// BufferFullWritingStrategy is the writing strategy when buffer is full
	BufferFullWritingStrategy dfv1.BufferFullWritingStrategy
}

// Option to apply different options
type Option interface {
	Apply(*Options)
}

// pipelining option
type pipelining bool

func (p pipelining) Apply(opts *Options) {
	opts.Pipelining = bool(p)
}

// WithoutPipelining turns on redis pipelining
func WithoutPipelining() Option {
	return pipelining(false)
}

// infoRefreshInterval option
type infoRefreshInterval time.Duration

func (i infoRefreshInterval) Apply(o *Options) {
	o.InfoRefreshInterval = time.Duration(i)
}

// WithInfoRefreshInterval sets the refresh interval
func WithInfoRefreshInterval(t time.Duration) Option {
	return infoRefreshInterval(t)
}

// lagDuration option
type lagDuration time.Duration

func (l lagDuration) Apply(o *Options) {
	o.LagDuration = time.Duration(l)
}

// WithLagDuration sets the consumerLag duration
func WithLagDuration(t time.Duration) Option {
	return lagDuration(t)
}

// readTimeOut option
type readTimeOut time.Duration

func (r readTimeOut) Apply(o *Options) {
	o.ReadTimeOut = time.Duration(r)
}

// WithReadTimeOut sets the readTimeOut
func WithReadTimeOut(t time.Duration) Option {
	return readTimeOut(t)
}

// checkBackLog option
type checkBackLog bool

func (b checkBackLog) Apply(o *Options) {
	o.CheckBackLog = bool(b)
}

// WithCheckBacklog sets the checkBackLog option
func WithCheckBacklog(b bool) Option {
	return checkBackLog(true)
}

// maxLength option
type maxLength int64

func (m maxLength) Apply(o *Options) {
	o.MaxLength = int64(m)
}

// WithMaxLength sets the maxLength
func WithMaxLength(m int64) Option {
	return maxLength(m)
}

// usageLimit option
type bufferUsageLimit float64

func (u bufferUsageLimit) Apply(o *Options) {
	o.BufferUsageLimit = float64(u)
}

// WithBufferUsageLimit sets the bufferUsageLimit
func WithBufferUsageLimit(u float64) Option {
	return bufferUsageLimit(u)
}

// refreshBufferWriteInfo option
type refreshBufferWriteInfo bool

func (r refreshBufferWriteInfo) Apply(o *Options) {
	o.RefreshBufferWriteInfo = bool(r)
}

// WithRefreshBufferWriteInfo sets the refreshBufferWriteInfo
func WithRefreshBufferWriteInfo(r bool) Option {
	return refreshBufferWriteInfo(r)
}

// WithBufferFullWritingStrategy option
type bufferFullWritingStrategy dfv1.BufferFullWritingStrategy

func (s bufferFullWritingStrategy) Apply(o *Options) {
	o.BufferFullWritingStrategy = dfv1.BufferFullWritingStrategy(s)
}

// WithBufferFullWritingStrategy sets the BufferFullWritingStrategy
func WithBufferFullWritingStrategy(s dfv1.BufferFullWritingStrategy) Option {
	return bufferFullWritingStrategy(s)
}
