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

package jetstream

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// isbReadErrors is used to indicate the number of errors in the jetstream READ operations
var isbReadErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "read_error_total",
	Help:      "Total number of jetstream read errors",
}, []string{"buffer"})

// isbFullErrors is used to indicate the number of errors in the jetstream isFull check
var isbFullErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "isFull_error_total",
	Help:      "Total number of jetstream isFull errors",
}, []string{"buffer"})

// isbFull is used to indicate the counter for number of times buffer is full
var isbFull = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "isFull_total",
	Help:      "Total number of IsFull",
}, []string{"buffer"})

// isbWriteErrors is used to indicate the number of errors in the jetstream write check
var isbWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "write_error_total",
	Help:      "Total number of jetstream write errors",
}, []string{"buffer"})

// isbSoftUsage is used to indicate of buffer that is used up, it is calculated based on the messages in pending + ack pending
var isbSoftUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_jetstream",
	Name:      "buffer_soft_usage",
	Help:      "percentage of buffer soft usage",
}, []string{"buffer"})

// isbSolidUsage is used to indicate of buffer that is used up, it is calculated based on the messages remain in the stream (if it's not Limits retention policy)
var isbSolidUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_jetstream",
	Name:      "buffer_solid_usage",
	Help:      "percentage of buffer solid usage",
}, []string{"buffer"})

// isbPending is calculated based on the messages in pending
var isbPending = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_jetstream",
	Name:      "buffer_pending",
	Help:      "number of pending messages",
}, []string{"buffer"})

// isbAckPending is calculated based on the messages that are pending ack
var isbAckPending = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "isb_jetstream",
	Name:      "buffer_ack_pending",
	Help:      "number of messages pending ack",
}, []string{"buffer"})

// isbWriteTimeout records how many times of writing timeout
var isbWriteTimeout = promauto.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "isb_jetstream",
	Name:      "write_timeout_total",
	Help:      "Total number of jetstream write timeouts",
}, []string{"buffer"})
