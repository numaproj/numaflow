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

package fetch

type options struct {
	// podHeartbeatRate uses second as time unit
	podHeartbeatRate int64
	// refreshingProcessorsRate uses second as time unit
	refreshingProcessorsRate int64
	// isReduce is true if the processor manager is for reduce. we have this because Reduce has a 1:1 mapping between
	// partitions and processors.
	isReduce bool
	// vertexReplica is the replica of the vertex
	vertexReplica int32
	// isSource is true if the vertex is source
	isSource bool
	// isFromVtxReduce is true if the  from vertex is reduce
	isFromVtxReduce bool
	// fromVtxPartitions is the number of partitions in from vertex.
	fromVtxPartitions int
}

// Option set options for FromVertex.
type Option func(options *options)

func defaultOptions() *options {
	return &options{
		podHeartbeatRate:         5,
		refreshingProcessorsRate: 5,
		vertexReplica:            0,
		fromVtxPartitions:        1,
	}
}

// WithPodHeartbeatRate sets the heartbeat rate in seconds.
func WithPodHeartbeatRate(rate int64) Option {
	return func(opts *options) {
		opts.podHeartbeatRate = rate
	}
}

// WithRefreshingProcessorsRate to set the rate of refreshing processors in seconds.
func WithRefreshingProcessorsRate(rate int64) Option {
	return func(opts *options) {
		opts.refreshingProcessorsRate = rate
	}
}

// WithIsReduce to indicate if the vertex is reduce.
func WithIsReduce(isReduce bool) Option {
	return func(opts *options) {
		opts.isReduce = isReduce
	}
}

// WithVertexReplica sets the vertex replica.
func WithVertexReplica(replica int32) Option {
	return func(opts *options) {
		opts.vertexReplica = replica
	}
}

// WithIsSource to indicate if the vertex is source.
func WithIsSource(isSource bool) Option {
	return func(opts *options) {
		opts.isSource = isSource
	}
}

// WithIsFromVtxReduce to indicate if the fromVertex is reduce
func WithIsFromVtxReduce(isFromVtxReduce bool) Option {
	return func(opts *options) {
		opts.isFromVtxReduce = isFromVtxReduce
	}
}

// WithFromVtxPartitions to indicate the number of partitions in fromVertex
func WithFromVtxPartitions(partitions int) Option {
	return func(opts *options) {
		opts.fromVtxPartitions = partitions
	}
}
