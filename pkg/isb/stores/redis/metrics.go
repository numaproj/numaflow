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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// isbIsEmptyFlagErrors is used to indicate the number of errors in the redis isEmpty check
var isbIsEmptyFlagErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_redis",
	Name:      "isEmpty_error_total",
	Help:      "Total number of Redis IsEmpty Errors",
}, []string{"buffer"})

// isbReadErrors is used to indicate the number of errors in the redis READ operations
var isbReadErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_redis",
	Name:      "read_error_total",
	Help:      "Total number of Redis IsEmpty Errors",
}, []string{"buffer"})

// isbIsFullErrors is used to indicate the number of errors in the redis isFull check
var isbIsFullErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_redis",
	Name:      "isFull_error_total",
	Help:      "Total number of Redis IsFull Errors",
}, []string{"buffer"})

// isbIsFull is used to indicate the counter for number of times buffer is full
var isbIsFull = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_redis",
	Name:      "isFull_total",
	Help:      "Total number of IsFull",
}, []string{"buffer"})

// isbIsEmpty is used to indicate the counter for number of times buffer is empty
var isbIsEmpty = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_redis",
	Name:      "isEmpty_total",
	Help:      "Total number of IsEmpty",
}, []string{"buffer"})

// isbWriteErrors is used to indicate the number of errors in the redis write check
var isbWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_redis",
	Name:      "write_error_total",
	Help:      "Total number of Redis Write Errors",
}, []string{"buffer"})

// isbBufferUsage is used to indicate of buffer that is used up
var isbBufferUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_redis",
	Name:      "buffer_usage",
	Help:      "% of buffer usage",
}, []string{"buffer"})

// isbConsumerLag is used to indicate the consumerLag
var isbConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_redis",
	Name:      "consumer_lag",
	Help:      "indicates consumer consumerLag",
}, []string{"buffer"})
