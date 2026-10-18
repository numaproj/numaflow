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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// PodReadRate is one pod's read rate (messages per second) over the fixed windows the fleet view shows.
// A nil window means the daemon has not yet observed that full lookback, so the caller omits it.
type PodReadRate struct {
	Hostname string
	Replica  int
	Rate5m   *float64
	Rate15m  *float64
}

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

// CalculatePodRates returns each active pod's read rate (messages per second) over lookbackSeconds.
// The map is nil when the queue does not yet cover the whole window, so a freshly started daemon
// does not report a 15 minute rate computed over a shorter interval. Only pods present in the latest
// complete snapshot are included; pods that have gone away are omitted.
func CalculatePodRates(q *sharedqueue.OverflowQueue[*TimestampedCounts], lookbackSeconds int64) map[string]float64 {
	counts := q.Items()
	if len(counts) <= 1 {
		return nil
	}
	// The oldest snapshot must be at least lookbackSeconds old. findStartIndex alone accepts a
	// shorter span as long as the second-to-last snapshot is inside the window.
	if counts[0].timestamp > time.Now().Unix()-lookbackSeconds {
		return nil
	}
	startIndex := findStartIndex(lookbackSeconds, counts)
	// The last snapshot may still be receiving pod updates, so the previous one is the end.
	endIndex := len(counts) - 2
	if startIndex == indexNotFound {
		return nil
	}
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff <= 0 {
		return nil
	}

	deltas := make(map[string]float64)
	for i := startIndex; i < endIndex; i++ {
		accumulatePodDeltas(counts[i], counts[i+1], deltas)
	}
	active := counts[endIndex].PodCountSnapshot()
	rates := make(map[string]float64, len(active))
	for podName := range active {
		rates[podName] = deltas[podName] / float64(timeDiff)
	}
	return rates
}

// accumulatePodDeltas adds each pod's count increase between two snapshots.
// A decrease is a counter reset: the current count is the increase since the restart.
func accumulatePodDeltas(tc1, tc2 *TimestampedCounts, deltas map[string]float64) {
	if tc1 == nil || tc2 == nil {
		return
	}
	prevCounts := tc1.PodCountSnapshot()
	for podName, currCount := range tc2.PodCountSnapshot() {
		prevCount := prevCounts[podName]
		podDelta := currCount
		if currCount >= prevCount {
			podDelta = currCount - prevCount
		}
		deltas[podName] += podDelta
	}
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

// podReadRates builds the fleet's 5m and 15m per-pod rates from one count timeline.
// A nil queue, or a window the timeline does not fully cover, leaves that window unset.
func podReadRates(q *sharedqueue.OverflowQueue[*TimestampedCounts]) []PodReadRate {
	if q == nil {
		return nil
	}
	return assemblePodReadRates(
		CalculatePodRates(q, fixedLookbackSeconds["5m"]),
		CalculatePodRates(q, fixedLookbackSeconds["15m"]),
	)
}

// assemblePodReadRates joins the two windows by pod hostname and orders the result by replica.
// Hostnames whose suffix is not a replica index are dropped; they cannot be matched to a Pod.
func assemblePodReadRates(rates5m, rates15m map[string]float64) []PodReadRate {
	names := make(map[string]struct{}, len(rates5m)+len(rates15m))
	for name := range rates5m {
		names[name] = struct{}{}
	}
	for name := range rates15m {
		names[name] = struct{}{}
	}
	if len(names) == 0 {
		return nil
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, leftOK := replicaFromHostname(ordered[i])
		right, rightOK := replicaFromHostname(ordered[j])
		if leftOK && rightOK && left != right {
			return left < right
		}
		return ordered[i] < ordered[j]
	})

	result := make([]PodReadRate, 0, len(ordered))
	for _, name := range ordered {
		replica, ok := replicaFromHostname(name)
		if !ok {
			continue
		}
		item := PodReadRate{Hostname: name, Replica: replica}
		if rate, found := rates5m[name]; rates5m != nil && found {
			copied := rate
			item.Rate5m = &copied
		}
		if rate, found := rates15m[name]; rates15m != nil && found {
			copied := rate
			item.Rate15m = &copied
		}
		result = append(result, item)
	}
	return result
}

// replicaFromHostname reads the replica index from a pod hostname such as "mv-mv-0".
func replicaFromHostname(hostname string) (int, bool) {
	dash := strings.LastIndex(hostname, "-")
	if dash < 0 || dash == len(hostname)-1 {
		return 0, false
	}
	replica, err := strconv.Atoi(hostname[dash+1:])
	if err != nil || replica < 0 {
		return 0, false
	}
	return replica, true
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
