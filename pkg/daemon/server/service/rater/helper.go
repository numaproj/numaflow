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
	"time"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// UpdateCount updates the count of processed messages for a pod at a given time
func UpdateCount(q *sharedqueue.OverflowQueue[*TimestampedCounts], time int64, podName string, count float64) {
	// find the element matching the input timestamp and update it
	for _, i := range q.Items() {
		if i.timestamp == time {
			i.Update(podName, count)
			return
		}
	}
	// if we cannot find a matching element, it means we need to add a new timestamped count to the queue
	tc := NewTimestampedCounts(time)
	tc.Update(podName, count)
	q.Append(tc)
}

// CalculateRate calculates the rate of the vertex in the last lookback seconds
func CalculateRate(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64) float64 {
	n := q.Length()
	if n <= 1 {
		return 0
	}
	counts := q.Items()
	startIndex := findStartIndex(lookbackSeconds, counts)
	if startIndex == -1 {
		return 0
	}

	delta := float64(0)
	// time diff in seconds.
	timeDiff := counts[n-1].timestamp - counts[startIndex].timestamp
	for i := startIndex; i < n-1; i++ {
		delta = delta + calculateDelta(counts[i], counts[i+1])
	}
	return delta / float64(timeDiff)
}

func calculateDelta(c1, c2 *TimestampedCounts) float64 {
	tc1 := c1.Snapshot()
	tc2 := c2.Snapshot()
	delta := float64(0)
	// Iterate over the podCounts of the second TimestampedCounts
	for pod, count2 := range tc2 {
		// If the pod also exists in the first TimestampedCounts
		if count1, ok := tc1[pod]; ok {
			// If the count has decreased, it means the pod restarted
			if count2 < count1 {
				delta += count2
			} else { // If the count has increased or stayed the same
				delta += count2 - count1
			}
		} else { // If the pod only exists in the second TimestampedCounts, it's a new pod
			delta += count2
		}
	}
	return delta
}

// CalculatePodRate calculates the rate of a pod in the last lookback seconds
func CalculatePodRate(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64, podName string) float64 {
	n := q.Length()
	if n <= 1 {
		return 0
	}
	counts := q.Items()
	startIndex := findStartIndex(lookbackSeconds, counts)
	if startIndex == -1 {
		return 0
	}

	delta := float64(0)
	// time diff in seconds.
	timeDiff := counts[n-1].timestamp - counts[startIndex].timestamp
	for i := startIndex; i < n-1; i++ {
		delta = delta + calculatePodDelta(counts[i], counts[i+1], podName)
	}
	return delta / float64(timeDiff)
}

func calculatePodDelta(c1, c2 *TimestampedCounts, podName string) float64 {
	tc1 := c1.Snapshot()
	tc2 := c2.Snapshot()
	count1, exist1 := tc1[podName]
	count2, exist2 := tc2[podName]
	if !exist2 {
		return 0
	} else if !exist1 {
		return count2
	} else if count2 < count1 {
		return count2
	} else {
		return count2 - count1
	}
}

// findStartIndex finds the index of the first element in the queue that is within the lookback seconds
// size of counts is at least 2
func findStartIndex(lookbackSeconds int64, counts []*TimestampedCounts) int {
	n := len(counts)
	now := time.Now().Truncate(time.Second * 10).Unix()
	if now-counts[n-2].timestamp > lookbackSeconds {
		// if the second last element is already outside the lookback window, we return an out of bound index
		return -1
	}

	startIndex := n - 2
	for i := n - 2; i >= 0; i-- {
		if now-counts[i].timestamp <= lookbackSeconds {
			startIndex = i
		} else {
			break
		}
	}
	return startIndex
}
