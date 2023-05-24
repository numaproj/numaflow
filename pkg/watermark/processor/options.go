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

package processor

type processorManagerOptions struct {
	// podHeartbeatRate uses second as time unit
	podHeartbeatRate int64
	// refreshingProcessorsRate uses second as time unit
	refreshingProcessorsRate int64
	// isReduce is true if the processor manager is for reduce. we have this because Reduce has a 1:1 mapping between
	// partitions and processors.
	isReduce bool
	// vertexReplica is the replica of the vertex
	vertexReplica int32
}

// ProcessorManagerOption set options for FromVertex.
type ProcessorManagerOption func(*processorManagerOptions)

// WithPodHeartbeatRate sets the heartbeat rate in seconds.
func WithPodHeartbeatRate(rate int64) ProcessorManagerOption {
	return func(opts *processorManagerOptions) {
		opts.podHeartbeatRate = rate
	}
}

// WithRefreshingProcessorsRate sets the processor refreshing rate in seconds.
func WithRefreshingProcessorsRate(rate int64) ProcessorManagerOption {
	return func(opts *processorManagerOptions) {
		opts.refreshingProcessorsRate = rate
	}
}

// WithIsReduce sets the processor manager is for reduce.
func WithIsReduce(isReduce bool) ProcessorManagerOption {
	return func(opts *processorManagerOptions) {
		opts.isReduce = isReduce
	}
}

// WithVertexReplica sets the vertex replica.
func WithVertexReplica(replica int32) ProcessorManagerOption {
	return func(opts *processorManagerOptions) {
		opts.vertexReplica = replica
	}
}
