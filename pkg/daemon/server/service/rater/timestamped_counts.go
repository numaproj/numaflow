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

package server

import (
	"fmt"
	"sync"
)

// CountNotAvailable indicates the rater is not able to collect the count of processed messages for a pod
// normally it is because the pod is not running
const CountNotAvailable = -1

// TimestampedCounts track the total count of processed messages for a list of pods at a given timestamp
type TimestampedCounts struct {
	// timestamp in seconds, is the time when the count is recorded
	timestamp int64
	// pod to partitionCount mapping
	podPartitionCount map[string]map[string]float64
	// isWindowClosed indicates whether we have finished collecting pod counts for this timestamp
	isWindowClosed bool
	lock           *sync.RWMutex
}

func NewTimestampedCounts(t int64) *TimestampedCounts {
	return &TimestampedCounts{
		timestamp:         t,
		podPartitionCount: make(map[string]map[string]float64),
		isWindowClosed:    false,
		lock:              new(sync.RWMutex),
	}
}

// Update updates the count for a pod if the current window is not closed
func (tc *TimestampedCounts) Update(podReadCount *PodReadCount) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if podReadCount == nil {
		// we choose to skip updating when podReadCount is nil, instead of removing the pod from the map.
		// imagine if the getPodReadCounts call fails to scrape the partitionReadCounts metric, and it's NOT because the pod is down.
		// in this case getPodReadCounts returns nil.
		// if we remove the pod from the map and then the next scrape successfully gets the partitionReadCounts, we can reach a state that in the timestamped counts,
		// for this single pod, at t1, partitionReadCounts is 123456, at t2, the map doesn't contain this pod and t3, partitionReadCounts is 123457.
		// when calculating the rate, as we sum up deltas among timestamps, we will get 123457 total delta instead of the real delta 1.
		// one occurrence of such case can lead to extremely high rate and mess up the autoscaling.
		// hence we'd rather keep the partitionReadCounts as it is to avoid wrong rate calculation.
		return
	}
	if tc.isWindowClosed {
		// we skip updating if the window is already closed.
		return
	}

	// since the pod can read from multiple partitions, we overwrite the previous partitionReadCounts for this pod
	// with the new partitionReadCounts map, since it is a counter metric, the new value is always greater than the previous one.
	tc.podPartitionCount[podReadCount.Name()] = podReadCount.PartitionReadCounts()
}

// PodReadCountSnapshot returns a copy of the podName to partitionCount mapping
// it's used to ensure the returned map is not modified by other goroutines
func (tc *TimestampedCounts) PodReadCountSnapshot() map[string]map[string]float64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	counts := make(map[string]map[string]float64)
	for k, v := range tc.podPartitionCount {
		counts[k] = v
	}
	return counts
}

// IsWindowClosed returns whether the window is closed
func (tc *TimestampedCounts) IsWindowClosed() bool {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return tc.isWindowClosed
}

// CloseWindow closes the window
func (tc *TimestampedCounts) CloseWindow() {
	// finalize the window by setting isWindowClosed to true
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.isWindowClosed = true
}

// ToString returns a string representation of the TimestampedCounts
// it's used for debugging purpose
func (tc *TimestampedCounts) ToString() string {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	res := fmt.Sprintf("{timestamp: %d, partitionCount: %v}", tc.timestamp, tc.podPartitionCount)
	return res
}
