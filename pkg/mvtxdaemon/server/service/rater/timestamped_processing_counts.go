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

// TimestampedProcessingTime track the total count of processed messages for a list of pods at a given timestamp
type TimestampedProcessingTime struct {
	// timestamp in seconds is the time when the count is recorded
	timestamp int64
	// the key of podProcessingTime represents the pod name, the value represents the batch processing time
	podProcessingTime map[string]float64
	lock              *sync.RWMutex
}

func NewTimestampedProcessingTime(t int64) *TimestampedProcessingTime {
	return &TimestampedProcessingTime{
		timestamp:         t,
		podProcessingTime: make(map[string]float64),
		lock:              new(sync.RWMutex),
	}
}

// Update updates the count of processed messages for a pod
func (tc *TimestampedProcessingTime) Update(podReadCount *PodProcessingTime) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if podReadCount == nil {
		return
	}
	sum, count := podReadCount.processingTimeValues()

	// Convert microseconds to seconds
	microseconds := sum / count
	seconds := microseconds / 1000000.0
	// convert this to nearest seconds
	tc.podProcessingTime[podReadCount.Name()] = seconds
}

// PodProcessingTimeSnapshot returns a copy of podProcessingTime
// it's used to ensure the returned map is not modified by other goroutines
func (tc *TimestampedProcessingTime) PodProcessingTimeSnapshot() map[string]float64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	counts := make(map[string]float64)
	for k, v := range tc.podProcessingTime {
		counts[k] = v
	}
	return counts
}

// String returns a string representation of the TimestampedProcessingTime
// it's used for debugging purpose
func (tc *TimestampedProcessingTime) String() string {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	return fmt.Sprintf("{timestamp: %d, podProcessingTime: %v}", tc.timestamp, tc.podProcessingTime)
}
