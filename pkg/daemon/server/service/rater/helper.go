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
func UpdateCount(q *sharedqueue.OverflowQueue[*TimestampedCounts], time int64, podReadCounts *PodReadCount) {
	items := q.Items()

	// find the element matching the input timestamp and update it
	for _, i := range items {
		if i.timestamp == time {
			i.Update(podReadCounts)
			return
		}
	}

	// if we cannot find a matching element, it means we need to add a new timestamped count to the queue
	tc := NewTimestampedCounts(time)
	tc.Update(podReadCounts)
	q.Append(tc)
}

// CalculateRate calculates the rate of the vertex partition in the last lookback seconds
func CalculateRate(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64, partitionName string) float64 {
	counts := q.Items()
	if len(counts) <= 1 {
		return 0
	}
	startIndex := findStartIndex(lookbackSeconds, counts)
	// we consider the last but one element as the end index because the last element might be incomplete
	// we can be sure that the last but one element in the queue is complete.
	endIndex := len(counts) - 2
	if startIndex == IndexNotFound {
		return 0
	}

	// time diff in seconds.
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff == 0 {
		// if the time difference is 0, we return 0 to avoid division by 0
		// this should not happen in practice because we are using a 10s interval
		return 0
	}

	if rate := getDeltaBetweenTimestampedCounts(counts[startIndex], counts[endIndex], partitionName) / float64(timeDiff); rate > 0 {
		// positive slope, meaning there was no restart in the last lookback seconds
		// TODO - FIX IT - the statement above doesn't always hold true.
		// see https://github.com/numaproj/numaflow/pull/810#discussion_r1261203309
		return rate
	}

	// maybe there was a restart, we need to iterate through the queue to compute the rate.
	delta := float64(0)
	for i := startIndex; i < endIndex; i++ {
		delta += calculatePartitionDelta(counts[i], counts[i+1], partitionName)
	}
	return delta / float64(timeDiff)
}

// getDeltaBetweenTimestampedCounts returns the total count changes between two timestamped counts for a partition
// by simply looping through the current pod list, comparing each pod read count with previous timestamped counts and summing up the deltas.
// getDeltaBetweenTimestampedCounts accepts negative deltas.
func getDeltaBetweenTimestampedCounts(t1, t2 *TimestampedCounts, partitionName string) float64 {
	delta := float64(0)
	if t1 == nil || t2 == nil {
		return delta
	}
	prevPodReadCount := t1.PodPartitionCountSnapshot()
	currPodReadCount := t2.PodPartitionCountSnapshot()
	for podName, partitionReadCounts := range currPodReadCount {
		delta += partitionReadCounts[partitionName] - prevPodReadCount[podName][partitionName]
	}
	return delta
}

// calculatePartitionDelta calculates the difference of the metric count between two timestamped counts for a given partition.
// calculatePartitionDelta doesn't accept negative delta, when encounters one, it treats it as a pod restart.
func calculatePartitionDelta(tc1, tc2 *TimestampedCounts, partitionName string) float64 {
	delta := float64(0)
	if tc1 == nil || tc2 == nil {
		// we calculate delta only when both input timestamped counts are non-nil
		return delta
	}
	prevPodReadCount := tc1.PodPartitionCountSnapshot()
	currPodReadCount := tc2.PodPartitionCountSnapshot()
	for podName, partitionReadCounts := range currPodReadCount {
		currCount := partitionReadCounts[partitionName]
		prevCount := prevPodReadCount[podName][partitionName]
		// pod delta will be equal to current count in case of restart
		podDelta := currCount
		if currCount >= prevCount {
			podDelta = currCount - prevCount
		}
		delta += podDelta
	}
	return delta
}

// findStartIndex finds the index of the first element in the queue that is within the lookback seconds
func findStartIndex(lookbackSeconds int64, counts []*TimestampedCounts) int {
	n := len(counts)
	now := time.Now().Truncate(CountWindow).Unix()
	if n < 2 || now-counts[n-2].timestamp > lookbackSeconds {
		// if the second last element is already outside the lookback window, we return IndexNotFound
		return IndexNotFound
	}

	startIndex := n - 2
	left := 0
	right := n - 2
	lastTimestamp := now - lookbackSeconds
	for left <= right {
		mid := left + (right-left)/2
		if counts[mid].timestamp >= lastTimestamp {
			startIndex = mid
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return startIndex
}
