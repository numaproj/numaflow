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

package rater

import (
	"fmt"
	"sync"
)

// PodTimeSeries represents a time series data point for a pod
type PodTimeSeries struct {
	// timestamp when the metric was fetched
	Time int64
	// value of the metric
	Value float64
}

// TimestampedCounts track the total count of processed messages for a list of pods at a given timestamp
type TimestampedCounts struct {
	// timestamp in seconds is the time when the count is recorded
	timestamp int64
	// the key represents the pod name, the value represents the time series data for the pod
	podTimeSeries map[string]*PodTimeSeries
	lock          *sync.RWMutex
}

func NewTimestampedCounts(t int64) *TimestampedCounts {
	return &TimestampedCounts{
		timestamp:     t,
		podTimeSeries: make(map[string]*PodTimeSeries),
		lock:          new(sync.RWMutex),
	}
}

// Update updates the count of processed messages for a pod
func (tc *TimestampedCounts) Update(podReadCount *PodMetricsCount) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if podReadCount == nil {
		// we choose to skip updating when podReadCounts is nil, instead of removing the pod from the map.
		// imagine if the getPodReadCounts call fails to scrape the readCount metric, and it's NOT because the pod is down.
		// in this case getPodReadCounts returns nil.
		// if we remove the pod from the map and then the next scrape successfully gets the readCount, we can reach a state that in the timestamped counts,
		// for this single pod, at t1, readCount is 123456, at t2, the map doesn't contain this pod and t3, readCount is 123457.
		// when calculating the rate, as we sum up deltas among timestamps, we will get 123457 total delta instead of the real delta 1.
		// one occurrence of such case can lead to extremely high rate and mess up the autoscaling.
		// hence we'd rather keep the readCount as it is to avoid wrong rate calculation.
		return
	}
	tc.podTimeSeries[podReadCount.Name()] = &PodTimeSeries{
		Time:  podReadCount.FetchTimestamp(),
		Value: podReadCount.ReadCount(),
	}
}

// PodTimeSeriesSnapshot returns a copy of podTimeSeries
// it's used to ensure the returned map is not modified by other goroutines
func (tc *TimestampedCounts) PodTimeSeriesSnapshot() map[string]*PodTimeSeries {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	// Create a deep copy to avoid concurrent access issues
	snapshot := make(map[string]*PodTimeSeries)
	for podName, timeSeries := range tc.podTimeSeries {
		snapshot[podName] = &PodTimeSeries{
			Time:  timeSeries.Time,
			Value: timeSeries.Value,
		}
	}
	return snapshot
}

// PodCountSnapshot returns a copy of pod counts for backward compatibility
// it's used to ensure the returned map is not modified by other goroutines
func (tc *TimestampedCounts) PodCountSnapshot() map[string]float64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	snapshot := make(map[string]float64)
	for podName, timeSeries := range tc.podTimeSeries {
		snapshot[podName] = timeSeries.Value
	}
	return snapshot
}

// String returns a string representation of the TimestampedCounts
// it's used for debugging purpose
func (tc *TimestampedCounts) String() string {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return fmt.Sprintf("{timestamp: %d, podTimeSeries: %v}", tc.timestamp, tc.podTimeSeries)
}

func (tc *TimestampedCounts) PodTimestamp() int64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return tc.timestamp
}
