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
)

// options for writing to redis
type options struct {
	// pipelining enables redis pipeline
	pipelining bool
	// infoRefreshInterval refreshes the info at this interval
	infoRefreshInterval time.Duration
	// lagDuration is the minimum permissable consumerLag that we consider as a valid consumerLag.
	lagDuration time.Duration
	// readTimeOut is the timeout needed for read timeout
	readTimeOut time.Duration
	// checkBackLog is used to read all the PENDING entries from the stream
	checkBackLog bool
	// maxLength is the maximum length of the stream before it reaches full
	maxLength int64
	// bufferUsageLimit is the limit of buffer usage before we declare it as full
	bufferUsageLimit float64
	// refreshBufferWriteInfo is used to determine if we refresh buffer write info
	refreshBufferWriteInfo bool
}

// Option to apply different options
type Option interface {
	apply(*options)
}

// pipelining option
type pipelining bool

func (p pipelining) apply(opts *options) {
	opts.pipelining = true
}

// WithoutPipelining turns on redis pipelining
func WithoutPipelining() Option {
	return pipelining(false)
}

// infoRefreshInterval option
type infoRefreshInterval time.Duration

func (i infoRefreshInterval) apply(o *options) {
	o.infoRefreshInterval = time.Duration(i)
}

// WithInfoRefreshInterval sets the refresh interval
func WithInfoRefreshInterval(t time.Duration) Option {
	return infoRefreshInterval(t)
}

// lagDuration option
type lagDuration time.Duration

func (l lagDuration) apply(o *options) {
	o.lagDuration = time.Duration(l)
}

// WithLagDuration sets the consumerLag duration
func WithLagDuration(t time.Duration) Option {
	return lagDuration(t)
}

// readTimeOut option
type readTimeOut time.Duration

func (r readTimeOut) apply(o *options) {
	o.readTimeOut = time.Duration(r)
}

// WithReadTimeOut sets the readTimeOut
func WithReadTimeOut(t time.Duration) Option {
	return readTimeOut(t)
}

// checkBackLog option
type checkBackLog bool

func (b checkBackLog) apply(o *options) {
	o.checkBackLog = true
}

// WithCheckBacklog sets the checkBackLog option
func WithCheckBacklog(b bool) Option {
	return checkBackLog(true)
}

// maxLength option
type maxLength int64

func (m maxLength) apply(o *options) {
	o.maxLength = int64(m)
}

// WithMaxLength sets the maxLength
func WithMaxLength(m int64) Option {
	return maxLength(m)
}

// usageLimit option
type bufferUsageLimit float64

func (u bufferUsageLimit) apply(o *options) {
	o.bufferUsageLimit = float64(u)
}

// WithBufferUsageLimit sets the bufferUsageLimit
func WithBufferUsageLimit(u float64) Option {
	return bufferUsageLimit(u)
}

// refreshBufferWriteInfo option
type refreshBufferWriteInfo bool

func (r refreshBufferWriteInfo) apply(o *options) {
	o.refreshBufferWriteInfo = bool(r)
}

// WithRefreshBufferWriteInfo sets the refreshBufferWriteInfo
func WithRefreshBufferWriteInfo(r bool) Option {
	return refreshBufferWriteInfo(r)
}
