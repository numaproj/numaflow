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

package redisstreams

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/numaproj/numaflow/pkg/metrics"
)

// Total number of Redis Streams messages Read
var redisStreamsSourceReadCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "redis_streams_source",
	Name:      "read_total",
	Help:      "Total number of Redis Streams messages Read",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// Total number of Redis Read Errors
var redisStreamsSourceReadErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "redis_streams_source",
	Name:      "read_err_total",
	Help:      "Total number of Redis IsEmpty Errors",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// Total number of Redis Streams messages Acknowledged
var redisStreamsSourceAckCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "redis_streams_source",
	Name:      "ack_total",
	Help:      "Total number of Redis Streams messages Acknowledged",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})

// Total number of Redis Ack Errors
var redisStreamsSourceAckErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "redis_streams_source",
	Name:      "ack_err_total",
	Help:      "Total number of Redis Streams messages that failed Ack",
}, []string{metrics.LabelVertex, metrics.LabelPipeline})
