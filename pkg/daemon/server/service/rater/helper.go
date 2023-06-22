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

const IndexNotFound = -1

// UpdateCount updates the count of processed messages for a pod at a given time
func UpdateCount(q *sharedqueue.OverflowQueue[*TimestampedCounts], time int64, podName string, count float64) {
	items := q.Items()

	// find the element matching the input timestamp and update it
	for _, i := range items {
		if i.timestamp == time {
			i.Update(podName, count)
			return
		}
	}

	// if we cannot find a matching element, it means we need to add a new timestamped count to the queue
	tc := NewTimestampedCounts(time)
	tc.Update(podName, count)

	// close the window for the most recent timestamped count
	switch n := len(items); n {
	case 0:
	// if the queue is empty, we just append the new timestamped count
	case 1:
		// if the queue has only one element, we close the window for this element
		items[0].CloseWindow(nil)
	default:
		// if the queue has more than one element, we close the window for the most recent element
		items[n-1].CloseWindow(items[n-2])
	}
	q.Append(tc)
}

// CalculateRate calculates the rate of the vertex in the last lookback seconds
func CalculateRate(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64) float64 {
	counts := q.Items()
	if len(counts) <= 1 {
		return 0
	}
	startIndex := findStartIndex(lookbackSeconds, counts)
	endIndex := findEndIndex(counts)
	if startIndex == IndexNotFound || endIndex == IndexNotFound {
		return 0
	}

	delta := float64(0)
	// time diff in seconds.
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff == 0 {
		// if the time difference is 0, we return 0 to avoid division by 0
		// this should not happen in practice because we are using a 10s interval
		return 0
	}
	for i := startIndex; i < endIndex; i++ {
		if counts[i+1] != nil && counts[i+1].IsWindowClosed() {
			delta += counts[i+1].delta
		}
	}
	return delta / float64(timeDiff)
}

// CalculatePodRate calculates the rate of a pod in the last lookback seconds
func CalculatePodRate(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64, podName string) float64 {
	counts := q.Items()
	if len(counts) <= 1 {
		return 0
	}
	startIndex := findStartIndex(lookbackSeconds, counts)
	endIndex := findEndIndex(counts)
	if startIndex == IndexNotFound || endIndex == IndexNotFound {
		return 0
	}

	delta := float64(0)
	// time diff in seconds.
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff == 0 {
		// if the time difference is 0, we return 0 to avoid division by 0
		// this should not happen in practice because we are using a 10s interval
		return 0
	}
	for i := startIndex; i < endIndex; i++ {
		if c1, c2 := counts[i], counts[i+1]; c1 != nil && c2 != nil && c1.IsWindowClosed() && c2.IsWindowClosed() {
			delta += calculatePodDelta(c1, c2, podName)
		}
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
	if n < 2 || now-counts[n-2].timestamp > lookbackSeconds {
		// if the second last element is already outside the lookback window, we return IndexNotFound
		return IndexNotFound
	}

	startIndex := n - 2
	for i := n - 2; i >= 0; i-- {
		if now-counts[i].timestamp <= lookbackSeconds && counts[i].IsWindowClosed() {
			startIndex = i
		} else {
			break
		}
	}
	return startIndex
}

func findEndIndex(counts []*TimestampedCounts) int {
	for i := len(counts) - 1; i >= 0; i-- {
		// if a window is not closed, we exclude it from the rate calculation
		if counts[i].IsWindowClosed() {
			return i
		}
	}
	return IndexNotFound
}
