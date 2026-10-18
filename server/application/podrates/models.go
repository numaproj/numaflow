/*
Copyright 2026 The Numaproj Authors.

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

// Package podrates serves per-pod read rates for the new Pod View fleet.
// Rates come from the vertex daemon's in-memory scrape. This package does not
// query Prometheus and does not list Pods.
package podrates

import "time"

// PodReadRate is one pod's read rate in messages per second.
// A nil window means the daemon has not yet observed that full lookback.
type PodReadRate struct {
	Replica  int
	Hostname string
	Rate5m   *float64
	Rate15m  *float64
}

// PodReadRates is the daemon's current per-pod read rates for one vertex.
type PodReadRates struct {
	ObservedAt time.Time
	Pods       []PodReadRate
}
