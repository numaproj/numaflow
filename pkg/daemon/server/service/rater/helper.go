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

	"github.com/numaproj/numaflow/pkg/isb"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const (
	indexNotFound = -1
	// rateNotAvailable is returned when the processing rate cannot be derived from the currently
	// available pod data, a negative min is returned to indicate this.
	rateNotAvailable = float64(math.MinInt)
)

// UpdatePendingCount updates the pending count for a pod at a given time
func UpdatePendingCount(q *sharedqueue.OverflowQueue[*TimestampedCounts], time int64, podPendingCounts *PodPendingCount) {
	items := q.Items()

	// find the element matching the input timestamp and update it
	for _, i := range items {
		if i.timestamp == time {
			i.UpdatePending(podPendingCounts)
			return
		}
	}

	// if we cannot find a matching element, it means we need to add a new timestamped count to the queue
	tc := NewTimestampedCounts(time)
	tc.UpdatePending(podPendingCounts)
	q.Append(tc)
}

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

func CalculatePending(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64, partitionName string) int64 {
	counts := q.Items()
	if len(counts) <= 1 {
		return isb.PendingNotAvailable
	}
	startIndex := findStartIndex(lookbackSeconds, counts)
	// we consider the last element as the end index
	endIndex := len(counts) - 1
	if startIndex == indexNotFound {
		return isb.PendingNotAvailable
	}
	sum := int64(0)
	num := int64(0)
	for i := startIndex; i <= endIndex; i++ {
		// Get the current pod partition map, and sum the pending values for given partition name
		currentPodPartitionMap := counts[i].PodPartitionCountSnapshot()
		for _, pendingCount := range currentPodPartitionMap {
			val, ok := pendingCount[partitionName]
			if ok {
				sum += int64(val)
				num++
			}
		}
	}
	if num == 0 {
		return isb.PendingNotAvailable
	}
	return sum / num
}

// CalculateRate calculates the rate of the vertex partition in the last lookback seconds
func CalculateRate(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64, partitionName string) float64 {
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
		delta += calculatePartitionDelta(counts[i], counts[i+1], partitionName)
	}
	return delta / float64(timeDiff)
}

// calculatePartitionDelta calculates the difference of the metric count between two timestamped counts for a given partition.
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

func processTimeline(counts []*TimestampedCounts, startIndex, endIndex int, data *podMaxDuration) {
	for i := startIndex; i <= endIndex; i++ {
		item := counts[i].PodPartitionCountSnapshot()
		curTime := counts[i].PodTimestamp()

		for podName, partitionCounts := range item {
			// Aggregate: sum all partition counts for this pod
			aggCount := float64(0)
			for _, count := range partitionCounts {
				aggCount += count
			}
			lastSeenData, found := data.lastSeen[podName]
			if found && lastSeenData.count == aggCount {
				continue
			}
			if found && aggCount > lastSeenData.count {
				duration := curTime - lastSeenData.seenTime
				if currentMax, ok := data.maxUnchangedDuration[podName]; !ok || duration > currentMax {
					data.maxUnchangedDuration[podName] = duration
				}
			}
			// The value is updated in the lastSeen for 3 cases
			// 1. If this is the first time seeing the pod entry or
			// 2. in case of a value increase,
			// 3. In case of a value decrease which is treated as a new entry for pod
			data.lastSeen[podName] = podLastSeen{aggCount, curTime}
		}
	}
}

// Check for pods that did not change at all during the iteration,
// and update their maxUnchangedDuration to the full period from first seen to lastTime.
// Note: There is a case where one pod was getting data earlier, but then stopped altogether.
// For example, one partition in Kafka not getting data after a while. This case will not be covered
// by our logic, and we would keep increasing the look back in such a scenario.
func finalizeDurations(lastCount *TimestampedCounts, data *podMaxDuration) {
	endVals := lastCount.PodPartitionCountSnapshot()
	lastTime := lastCount.PodTimestamp()
	for key, lastSeenData := range data.lastSeen {
		endDuration := lastTime - lastSeenData.seenTime
		// This condition covers two scenarios:
		// 1. There is an entry in the last seen, but not in maxUnchangedDuration
		// It was seen once, but value never changed. In this case update the maxDuration, but only when
		// the count > 0
		// 2. The value has not changed till the boundary, and this duration is larger than the current max
		if _, exists := endVals[key]; exists && lastSeenData.count != 0 {
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
