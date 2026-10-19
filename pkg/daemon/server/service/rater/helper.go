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
	indexNotFound = -1
	// rateNotAvailable is returned when the processing rate cannot be derived from the currently
	// available pod data, a negative min is returned to indicate this.
	rateNotAvailable = float64(math.MinInt)
)

// CalculatePending calculates the pending messages for a given partition in the last lookback seconds
func CalculatePending(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64, partitionName string) int64 {
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
	sum := int64(0)
	num := int64(0)
	for i := startIndex; i <= endIndex; i++ {
		// We maintain the map only for 0th replica for all the vertices except Reduce.
		// So currentPodPartitionMap will only have entry for one pod name except in case of Reduce.
		// Even for Reduce, num would turn out to be 1, as there is 1-1 mapping between partition and pod.
		/*
			Eg: For Non-reduce vertices, currentPodPartitionMap:
				map[
					simple-pipeline-cat-0:map[ // pod with replica 0, no entry for pod with replica 1
						default-simple-pipeline-cat-0:331 // first partition
						default-simple-pipeline-cat-1:364 // second partition
					]
				]
			Eg: For Reduce vertices, currentPodPartitionMap:
				map[
					simple-pipeline-reduce-0:map[ // pod with replica 0
						default-simple-pipeline-reduce-0:331 // first partition
					]
					simple-pipeline-reduce-1:map[ // pod with replica 1
						default-simple-pipeline-reduce-1:364 // second partition
					]
				]
		*/
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
		return v1alpha1.PendingNotAvailable
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

type partitionLastSeen struct {
	count    float64
	seenTime int64
}

type partitionMaxDuration struct {
	// Map to keep track of the last seen count and timestamp of each partition.
	lastSeen map[string]partitionLastSeen
	// Map to store the maximum duration for which the value of any partition was unchanged.
	maxUnchangedDuration map[string]int64
}

// CalculateLookback computes the maximum duration (in seconds) for which the count of messages processed across
// all the partitions of a given vertex remain unchanged. This helps determine how long the system should
// look back when calculating processing rates.
//
// The function analyzes timestamped count data to find the longest period during which any partition
// had no change in its processed message count. This is useful for two scenarios:
// 1. Slow processing vertices - where a vertex takes a long time to process messages
// 2. Slow data sources - where data arrives infrequently, causing long gaps between count changes
//
// The returned duration represents the maximum "unchanged period" across all partitions, which
// helps the system adjust its lookback window to ensure it captures enough data for accurate
// rate calculations.
func CalculateLookback(counts []*TimestampedCounts, startIndex, endIndex int) int64 {
	lookBackData := partitionMaxDuration{
		lastSeen:             make(map[string]partitionLastSeen),
		maxUnchangedDuration: make(map[string]int64),
	}
	processTimeline(counts, startIndex, endIndex, &lookBackData)
	finalizeDurations(counts[endIndex], &lookBackData)
	return findGlobalMaxDuration(lookBackData.maxUnchangedDuration)
}

// processTimeline analyzes the timeline of count data to track how long each partition's count
// remained unchanged. It processes each timestamped count entry and updates the maximum
// unchanged duration for each partition when their counts change.
func processTimeline(counts []*TimestampedCounts, startIndex, endIndex int, data *partitionMaxDuration) {
	for i := startIndex; i <= endIndex; i++ {
		podPartitionCountMap := counts[i].PodPartitionCountSnapshot()
		curTime := counts[i].PodTimestamp()

		// Aggregate counts across all pods for each partition at this timestamp
		// This gives us the total count for each partition at this point in time
		partitionAggMap := make(map[string]float64)
		for _, partitionCountsMap := range podPartitionCountMap {
			for partitionName, count := range partitionCountsMap {
				partitionAggMap[partitionName] += count
			}
		}

		// Process each partition's aggregated count
		for partitionName, aggCount := range partitionAggMap {
			lastSeenData, found := data.lastSeen[partitionName]

			// Skip if the count hasn't changed since we last saw it
			if found && lastSeenData.count == aggCount {
				continue
			}

			// If we've seen this partition before and the count has increased,
			// calculate how long it remained unchanged and update the max duration
			if found && aggCount > lastSeenData.count {
				duration := curTime - lastSeenData.seenTime
				if currentMax, ok := data.maxUnchangedDuration[partitionName]; !ok || duration > currentMax {
					data.maxUnchangedDuration[partitionName] = duration
				}
			}

			// Update the last seen data for this partition. This happens in three cases:
			// 1. First time seeing this partition
			// 2. Count increased
			// 3. Count decreased (treated as a new entry)
			data.lastSeen[partitionName] = partitionLastSeen{aggCount, curTime}
		}
	}
}

// finalizeDurations calculates the final unchanged durations for partitions that remained
// unchanged until the end of the analysis period.
func finalizeDurations(lastCount *TimestampedCounts, data *partitionMaxDuration) {
	endVals := lastCount.PodPartitionCountSnapshot()
	lastTime := lastCount.PodTimestamp()

	// Aggregate counts across all pods for each partition at the final timestamp
	// This gives us the total count for each partition at the end of the analysis period
	partitionAggMap := make(map[string]float64)
	for _, partitionCounts := range endVals {
		for partitionName, count := range partitionCounts {
			partitionAggMap[partitionName] += count
		}
	}

	// For each partition check if it remained unchanged until the end and
	// update the max unchanged duration accordingly
	for partitionName, lastSeenData := range data.lastSeen {
		// Calculate how long this partition's count remained unchanged from when it was
		// last seen until the final timestamp
		endDuration := lastTime - lastSeenData.seenTime

		// Only update the max unchanged duration if:
		// 1. The partition still exists in the final data (hasn't been removed)
		// 2. The partition had a non-zero count when it was last seen
		// 3. The calculated duration is longer than any previously recorded duration for this partition
		if _, exists := partitionAggMap[partitionName]; exists && lastSeenData.count != 0 {
			if currentMax, ok := data.maxUnchangedDuration[partitionName]; !ok || endDuration > currentMax {
				data.maxUnchangedDuration[partitionName] = endDuration
			}
		}
	}
}

// findGlobalMaxDuration finds the maximum unchanged duration across all partitions.
// This represents the longest period during which any partition had no change in its
// processed message count.
func findGlobalMaxDuration(maxUnchangedDuration map[string]int64) int64 {
	globalMaxSecs := int64(0)
	for _, duration := range maxUnchangedDuration {
		if duration > globalMaxSecs {
			globalMaxSecs = duration
		}
	}
	return globalMaxSecs
}
