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

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const (
	// indexNotFound is returned when the start index cannot be found in the queue.
	indexNotFound = -1
	// rateNotAvailable is returned when the processing rate cannot be derived from the currently
	// available pod data, a negative min is returned to indicate this.
	rateNotAvailable = float64(math.MinInt)
)

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

// CalculatePending calculates the pending of a MonoVertex for a given lookback period.
func CalculatePending(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64) int64 {
	counts := q.Items()
	if len(counts) <= 1 {
		return v1alpha1.PendingNotAvailable
	}
	startIndex := findStartIndex(lookbackSeconds, counts)
	// we consider the last element as the end index
	endIndex := len(counts) - 1
	if startIndex == indexNotFound {
		return v1alpha1.PendingNotAvailable
	}
	delta := int64(0)
	num := int64(0)
	for i := startIndex; i <= endIndex; i++ {
		currentPending := counts[i].PodCountSnapshot()
		for _, pendingCount := range currentPending {
			delta += int64(pendingCount)
			num++
		}
	}
	if num == 0 {
		return v1alpha1.PendingNotAvailable
	}
	return delta / num
}

// findStartIndex finds the index of the first element in the queue that is within the lookback seconds
func findStartIndex(lookbackSeconds int64, counts []*TimestampedCounts) int {
	n := len(counts)
	now := time.Now().Unix()
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

// podLastSeen stores the last seen timestamp and count of each pod
type podLastSeen struct {
	count    float64
	seenTime int64
}

type podMaxDuration struct {
	// Map to keep track of the last seen count and timestamp of each pod.
	lastSeen map[string]podLastSeen
	// Map to store the maximum duration for which the value of any pod was unchanged.
	maxUnchangedDuration map[string]int64
}

// CalculateMaxLookback computes the maximum duration (in seconds) for which the count of messages processed by any pod
// remained unchanged within a specified range of indices in a queue of TimestampedCounts. It does this by analyzing each
// data point between the startIndex and endIndex, checking the count changes for each pod, and noting the durations
// during which these counts stay consistent. The metric is updated when data is read by the pod
// This would encapsulate the lookback for two scenarios
// 1. Slow processing vertex
// 2. Slow data source - data arrives after long intervals
func CalculateMaxLookback(counts []*TimestampedCounts, startIndex, endIndex int) int64 {
	lookBackData := podMaxDuration{
		lastSeen:             make(map[string]podLastSeen),
		maxUnchangedDuration: make(map[string]int64),
	}
	processTimeline(counts, startIndex, endIndex, &lookBackData)
	finalizeDurations(counts[endIndex], &lookBackData)
	return findGlobalMaxDuration(lookBackData.maxUnchangedDuration)
}

// processTimeline processes the timeline of counts and updates the maxUnchangedDuration for each pod.
func processTimeline(counts []*TimestampedCounts, startIndex, endIndex int, data *podMaxDuration) {
	for i := startIndex; i <= endIndex; i++ {
		item := counts[i].PodCountSnapshot()
		curTime := counts[i].PodTimestamp()

		for key, curCount := range item {
			lastSeenData, found := data.lastSeen[key]
			if found && lastSeenData.count == curCount {
				continue
			}

			// If the read count data has updated
			if found && curCount > lastSeenData.count {
				duration := curTime - lastSeenData.seenTime
				if currentMax, ok := data.maxUnchangedDuration[key]; !ok || duration > currentMax {
					data.maxUnchangedDuration[key] = duration
				}
			}
			// The value is updated in the lastSeen for 3 cases
			// 1. If this is the first time seeing the pod entry or
			// 2. in case of a value increase,
			// 3. In case of a value decrease which is treated as a new entry for pod
			data.lastSeen[key] = podLastSeen{curCount, curTime}
		}
	}
}

// Check for pods that did not change at all during the iteration,
// and update their maxUnchangedDuration to the full period from first seen to lastTime.
// Note: There is a case where one pod was getting data earlier, but then stopped altogether.
// For example, one partition in Kafka not getting data after a while. This case will not be covered
// by our logic, and we would keep increasing the look back in such a scenario.
func finalizeDurations(lastCount *TimestampedCounts, data *podMaxDuration) {
	endVals := lastCount.PodCountSnapshot()
	lastTime := lastCount.PodTimestamp()
	for key, lastSeenData := range data.lastSeen {
		endDuration := lastTime - lastSeenData.seenTime
		// This condition covers two scenarios:
		// 1. There is an entry in the last seen, but not in maxUnchangedDuration
		// It was seen once, but value never changed. In this case update the maxDuration, but only when
		// the count > 0
		// 2. The value has not changed till the boundary, and this duration is larger than the current max
		if _, exists := endVals[key]; exists && (lastSeenData.count != 0) {
			if currentMax, ok := data.maxUnchangedDuration[key]; !ok || endDuration > currentMax {
				data.maxUnchangedDuration[key] = endDuration
			}
		}
	}
}

// Calculate the maximum duration found across all pods.
func findGlobalMaxDuration(maxUnchangedDuration map[string]int64) int64 {
	globalMaxSecs := int64(0)
	for _, duration := range maxUnchangedDuration {
		if duration > globalMaxSecs {
			globalMaxSecs = duration
		}
	}
	return globalMaxSecs
}
