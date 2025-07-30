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
	"maps"
	"sync"
)

// TimestampedCounts track the total count of processed messages for a list of pods at a given timestamp
type TimestampedCounts struct {
	// timestamp in seconds is the time when the count is recorded
	timestamp int64
	// the key of podPartitionCount represents the pod name, the value represents a partition counts map for the pod
	// partition counts map holds mappings between partition name and the count of messages processed by the partition
	podPartitionCount map[string]map[string]float64
	lock              *sync.RWMutex
}

func NewTimestampedCounts(t int64) *TimestampedCounts {
	return &TimestampedCounts{
		timestamp:         t,
		podPartitionCount: make(map[string]map[string]float64),
		lock:              new(sync.RWMutex),
	}
}

func (tc *TimestampedCounts) PodTimestamp() int64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return tc.timestamp
}

func (tc *TimestampedCounts) UpdatePending(podPendingCount *PodPendingCount) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if podPendingCount == nil {
		return
	}
	tc.podPartitionCount[podPendingCount.Name()] = podPendingCount.PartitionPendingCounts()
}

// Update updates the count of processed messages for a pod
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
	tc.podPartitionCount[podReadCount.Name()] = podReadCount.PartitionReadCounts()
}

// PodPartitionCountSnapshot returns a copy of podPartitionCount
// it's used to ensure the returned map is not modified by other goroutines
func (tc *TimestampedCounts) PodPartitionCountSnapshot() map[string]map[string]float64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	counts := make(map[string]map[string]float64)
	maps.Copy(counts, tc.podPartitionCount)
	return counts
}

// String returns a string representation of the TimestampedCounts
// it's used for debugging purpose
func (tc *TimestampedCounts) String() string {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return fmt.Sprintf("{timestamp: %d, podPartitionCount: %v}", tc.timestamp, tc.podPartitionCount)
}
