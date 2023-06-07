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
	// podName to count mapping
	podCounts map[string]float64
	lock      *sync.RWMutex
}

func NewTimestampedCounts(t int64) *TimestampedCounts {
	return &TimestampedCounts{
		timestamp: t,
		podCounts: make(map[string]float64),
		lock:      new(sync.RWMutex),
	}
}

func (tc *TimestampedCounts) Update(podName string, count float64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if count == CountNotAvailable {
		// we choose to skip updating when count is not available for the pod, instead of removing the pod from the map.
		// imagine if the getTotalCount call fails to scrape the count metric, and it's NOT because the pod is down.
		// in this case getTotalCount returns CountNotAvailable.
		// if we remove the pod from the map and then the next scrape successfully gets the count, we can reach a state that in the timestamped counts,
		// for this single pod, at t1, count is 123456, at t2, the map doesn't contain this pod and t3, count is 123457.
		// when calculating the rate, as we sum up deltas among timestamps, we will get 123457 total delta instead of the real delta 1.
		// one occurrence of such case can lead to extremely high rate and mess up the autoscaling.
		// hence we'd rather keep the count as it is to avoid wrong rate calculation.
		return
	}
	tc.podCounts[podName] = count
}

// Snapshot returns a copy of the podName to count mapping
// it's used to ensure the returned map is not modified by other goroutines
func (tc *TimestampedCounts) Snapshot() map[string]float64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	counts := make(map[string]float64)
	for k, v := range tc.podCounts {
		counts[k] = v
	}
	return counts
}

// ToString returns a string representation of the TimestampedCounts
// it's used for debugging purpose
func (tc *TimestampedCounts) ToString() string {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	res := fmt.Sprintf("{timestamp: %d, podCount: %v}", tc.timestamp, tc.podCounts)
	return res
}
