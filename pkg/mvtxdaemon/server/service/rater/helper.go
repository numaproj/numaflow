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
	"math"
	"time"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const (
	// indexNotFound is returned when the start index cannot be found in the queue.
	indexNotFound = -1
	// rateNotAvailable is returned when the processing rate cannot be derived from the currently
	// available pod data, a negative min is returned to indicate this.
	rateNotAvailable = float64(math.MinInt)
)

// UpdateCount updates the count for a given timestamp in the queue.
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

// CalculateRate calculates the rate of a MonoVertex for a given lookback period.
func CalculateRate(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64) float64 {
	counts := q.Items()
	if len(counts) <= 1 {
		return rateNotAvailable
	}
	startIndex := findStartIndex(lookbackSeconds, counts)
	// we consider the last but one element as the end index because the last element might be incomplete
	// we can be sure that the last but one element in the queue is complete.
	endIndex := len(counts) - 2
	if startIndex == indexNotFound {
		return rateNotAvailable
	}

	// time diff in seconds.
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff == 0 {
		// if the time difference is 0, we return 0 to avoid division by 0
		// this should not happen in practice because we are using a 10s interval
		return rateNotAvailable
	}

	delta := float64(0)
	for i := startIndex; i < endIndex; i++ {
		// calculate the difference between the current and previous pod count snapshots
		delta += calculatePodDelta(counts[i], counts[i+1])
	}
	return delta / float64(timeDiff)
}

// findStartIndex finds the index of the first element in the queue that is within the lookback seconds
func findStartIndex(lookbackSeconds int64, counts []*TimestampedCounts) int {
	n := len(counts)
	now := time.Now().Truncate(CountWindow).Unix()
	if n < 2 || now-counts[n-2].timestamp > lookbackSeconds {
		// if the second last element is already outside the lookback window, we return indexNotFound
		return indexNotFound
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

// calculatePodDelta calculates the difference between the current and previous pod count snapshots
func calculatePodDelta(tc1, tc2 *TimestampedCounts) float64 {
	delta := float64(0)
	if tc1 == nil || tc2 == nil {
		// we calculate delta only when both input timestamped counts are non-nil
		return delta
	}
	prevPodReadCount := tc1.PodCountSnapshot()
	currPodReadCount := tc2.PodCountSnapshot()
	for podName, readCount := range currPodReadCount {
		currCount := readCount
		prevCount := prevPodReadCount[podName]
		// pod delta will be equal to current count in case of restart
		podDelta := currCount
		if currCount >= prevCount {
			podDelta = currCount - prevCount
		}
		delta += podDelta
	}
	return delta
}

// CalculateMaxLookback computes the maximum duration (in seconds) for which the count of messages processed by any pod
// remained unchanged within a specified range of indices in a queue of TimestampedCounts. It does this by analyzing each
// data point between the startIndex and endIndex, checking the count changes for each pod, and noting the durations
// during which these counts stay consistent. The metric is updated when data is read by the pod
// This would encapsulate the lookback for two scenarios
// 1. Slow processing vertex
// 2. Slow data source - data arrives after long intervals
func CalculateMaxLookback(counts []*TimestampedCounts, startIndex, endIndex int) int64 {
	// Map to keep track of the last seen count and timestamp of each pod.
	lastSeen := make(map[string]struct {
		count    float64
		seenTime int64
	})

	// Map to store the maximum duration for which the value of any pod was unchanged.
	maxUnchangedDuration := make(map[string]int64)

	for i := startIndex; i < endIndex; i++ {
		// Get a snapshot of pod counts and the timestamp for the current index.
		item := counts[i].PodCountSnapshot()
		curTime := counts[i].PodTimestamp()

		// Iterate through each pod in the snapshot.
		for key, curCount := range item {
			lastSeenData, found := lastSeen[key]
			if found && lastSeenData.count == curCount {
				continue
			}
			// If the read count data has updated
			if found && curCount > lastSeenData.count {
				// Calculate the duration for which the count was unchanged.
				duration := curTime - lastSeenData.seenTime
				// Update maxUnchangedDuration for the pod if this duration is the longest seen so far.
				// TODO: Can check if average or EWMA works better than max
				if currentMax, ok := maxUnchangedDuration[key]; !ok || duration > currentMax {
					maxUnchangedDuration[key] = duration
				}
			}
			// The value is updated in the lastSeen for 3 cases
			// 1. If this is the first time seeing the pod entry or
			// 2. in case of a value increase,
			// 3. In case of a value decrease which is treated as a new entry for pod
			lastSeen[key] = struct {
				count    float64
				seenTime int64
			}{curCount, curTime}

		}
	}

	// Fetch the last timestamp used in the analysis to check unmodified runs.
	endVals := counts[endIndex].PodCountSnapshot()
	lastTime := counts[endIndex].PodTimestamp()

	// Check for pods that did not change at all during the iteration,
	// and update their maxUnchangedDuration to the full period from first seen to lastTime.
	for key, data := range lastSeen {
		if _, ok := maxUnchangedDuration[key]; !ok {
			if _, found := endVals[key]; found {
				maxUnchangedDuration[key] = lastTime - data.seenTime
			}
		}
	}

	// Calculate the maximum duration found across all pods.
	globalMaxSecs := int64(0)
	for _, duration := range maxUnchangedDuration {
		if duration > globalMaxSecs {
			globalMaxSecs = duration
		}
	}
	return globalMaxSecs
}
