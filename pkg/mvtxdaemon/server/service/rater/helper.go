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
	"sync"
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

// TimeSeries represents a single data point with timestamp and value
type TimeSeries struct {
	Time  int64   // fetch timestamp from HTTP Date header
	Value float64 // metric value
}

// PodRateData maintains time series data for rate calculation
type PodRateData struct {
	// map[pod-replica] -> []*TimeSeries (sorted by time, acts as overflow queue)
	podTimeSeries map[string][]*TimeSeries
	mutex         sync.RWMutex
}

// NewPodRateData creates a new PodRateData instance
func NewPodRateData() *PodRateData {
	return &PodRateData{
		podTimeSeries: make(map[string][]*TimeSeries),
	}
}

// AddDataPoint adds a new data point for a pod, maintaining sorted order
func (prd *PodRateData) AddDataPoint(podName string, timestamp int64, value float64) {
	prd.mutex.Lock()
	defer prd.mutex.Unlock()

	newPoint := &TimeSeries{Time: timestamp, Value: value}

	if _, exists := prd.podTimeSeries[podName]; !exists {
		prd.podTimeSeries[podName] = []*TimeSeries{newPoint}
		return
	}

	// Since timestamps come from real-time HTTP Date headers, they are naturally sorted
	// Just append to maintain chronological order
	prd.podTimeSeries[podName] = append(prd.podTimeSeries[podName], newPoint)
}

// GetActivePods returns the list of pods that have data
func (prd *PodRateData) GetActivePods() []string {
	prd.mutex.RLock()
	defer prd.mutex.RUnlock()

	pods := make([]string, 0, len(prd.podTimeSeries))
	for podName := range prd.podTimeSeries {
		if len(prd.podTimeSeries[podName]) > 0 {
			pods = append(pods, podName)
		}
	}
	return pods
}



// CalculateRateFromData calculates rate for all pods using the new data structure
func (prd *PodRateData) CalculateRateFromData(lookbackSeconds int64) float64 {
	prd.mutex.RLock()
	defer prd.mutex.RUnlock()

	if len(prd.podTimeSeries) == 0 {
		return rateNotAvailable
	}

	totalRate := float64(0)

	// Iterate over all pods and calculate their rates
	// Each pod looks back from its own latest timestamp
	for podName := range prd.podTimeSeries {
		podRate := prd.calculateSinglePodRate(podName, lookbackSeconds)
		totalRate += podRate
	}

	return math.Floor(totalRate)
}

// calculateSinglePodRate calculates rate for a single pod (caller must hold lock)
func (prd *PodRateData) calculateSinglePodRate(podName string, lookbackSeconds int64) float64 {
	series, exists := prd.podTimeSeries[podName]
	if !exists || len(series) < 2 {
		return 0 // Need at least 2 data points
	}

	// Calculate lookback time from this pod's latest timestamp
	latestTime := series[len(series)-1].Time
	lookbackTime := latestTime - lookbackSeconds

	startIndex := prd.findStartIndex(podName, lookbackTime)
	if startIndex == -1 || startIndex >= len(series)-1 {
		return 0 // No valid data within lookback window
	}

	// Calculate total delta from start index to latest
	totalDelta := float64(0)
	for i := startIndex; i < len(series)-1; i++ {
		curr := series[i+1]
		prev := series[i]

		// Calculate delta (handle restarts)
		delta := curr.Value
		if curr.Value >= prev.Value {
			// Normal case: count increased
			delta = curr.Value - prev.Value
		} else {
			// Restart case: use current value as delta
			delta = curr.Value
		}

		totalDelta += delta
	}

	// Rate = total_delta / lookback_seconds
	return totalDelta / float64(lookbackSeconds)
}

// findStartIndex finds the start index for a pod within the lookback window using binary search
// (caller must hold lock)
func (prd *PodRateData) findStartIndex(podName string, lookbackTime int64) int {
	series, exists := prd.podTimeSeries[podName]
	if !exists || len(series) == 0 {
		return -1
	}

	// Binary search for the first element >= lookbackTime
	left, right := 0, len(series)
	for left < right {
		mid := (left + right) / 2
		if series[mid].Time < lookbackTime {
			left = mid + 1
		} else {
			right = mid
		}
	}

	if left >= len(series) {
		return -1 // No data within lookback window
	}

	return left
}

// CleanupOldData removes entries older than maxLookbackSeconds from all pods
func (prd *PodRateData) CleanupOldData(maxLookbackSeconds int64) {
	prd.mutex.Lock()
	defer prd.mutex.Unlock()

	if len(prd.podTimeSeries) == 0 {
		return
	}

	// Find the latest timestamp across all pods
	latestTime := int64(0)
	for _, series := range prd.podTimeSeries {
		if len(series) > 0 {
			if series[len(series)-1].Time > latestTime {
				latestTime = series[len(series)-1].Time
			}
		}
	}

	if latestTime == 0 {
		return
	}

	cutoffTime := latestTime - maxLookbackSeconds

	// Clean up old entries for each pod
	for podName, series := range prd.podTimeSeries {
		if len(series) == 0 {
			// Remove pods with no data
			delete(prd.podTimeSeries, podName)
			continue
		}

		// Find the first index to keep (>= cutoffTime)
		keepIndex := 0
		for i, point := range series {
			if point.Time >= cutoffTime {
				keepIndex = i
				break
			}
			keepIndex = i + 1
		}

		// If all entries are old, keep only the latest one to maintain continuity
		if keepIndex >= len(series) {
			if len(series) > 1 {
				prd.podTimeSeries[podName] = series[len(series)-1:]
			}
		} else if keepIndex > 0 {
			// Keep entries from keepIndex onwards
			prd.podTimeSeries[podName] = series[keepIndex:]
		}

		// Remove pods that have no recent data
		if len(prd.podTimeSeries[podName]) == 0 {
			delete(prd.podTimeSeries, podName)
		}
	}
}

// UpdateCount updates the count for a given timestamp in the queue.
func UpdateCount(q *sharedqueue.OverflowQueue[*TimestampedCounts], time int64, podReadCounts *PodMetricsCount) {
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

	// Calculate rate per pod using lookback-based approach and then sum across pods
	rate := calculatePodRatesWithLookback(counts, lookbackSeconds)
	return math.Floor(rate)
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

// calculatePodRatesWithLookback calculates rate for each active pod using lookback approach and sums them
func calculatePodRatesWithLookback(counts []*TimestampedCounts, lookbackSeconds int64) float64 {
	if len(counts) == 0 {
		return 0
	}

	// Get active pods from the latest timestamp
	latestCounts := counts[len(counts)-1]
	activePods := latestCounts.PodTimeSeriesSnapshot()

	totalRate := float64(0)

	// Calculate rate for each active pod separately
	for podName := range activePods {
		podRate := calculateSinglePodRateWithLookback(counts, podName, lookbackSeconds)
		totalRate += podRate
	}

	return totalRate
}

// calculateSinglePodRateWithLookback calculates the rate for a single pod using lookback approach
func calculateSinglePodRateWithLookback(counts []*TimestampedCounts, podName string, lookbackSeconds int64) float64 {
	if len(counts) == 0 {
		return 0
	}

	// Find the pod-specific start index based on lookback time
	podStartIndex := findPodStartIndex(counts, podName, lookbackSeconds)
	if podStartIndex == -1 {
		return 0 // Pod not found or insufficient data
	}

	podDelta := float64(0)

	// Calculate total delta for this pod from its start index to end
	for i := podStartIndex; i < len(counts)-1; i++ {
		prevPodTimeSeries := counts[i].PodTimeSeriesSnapshot()
		currPodTimeSeries := counts[i+1].PodTimeSeriesSnapshot()

		currData, currExists := currPodTimeSeries[podName]
		prevData, prevExists := prevPodTimeSeries[podName]

		if !currExists {
			continue
		}

		// pod delta will be equal to current count in case of restart or new pod
		windowDelta := currData.Value
		if prevExists && currData.Value >= prevData.Value {
			// Normal case: pod existed before and count increased
			windowDelta = currData.Value - prevData.Value
		} else if !prevExists {
			// New pod case: use 0 delta to prevent cold start spike
			// The accumulated count represents historical processing, not recent activity
			windowDelta = 0
		}

		// If currCount < prevCount and pod existed before, use currCount (restart case)
		podDelta += windowDelta
	}

	// Calculate rate: total_delta / lookback_seconds
	// This ensures new pods contribute their full rate potential
	return podDelta / float64(lookbackSeconds)
}

// findPodStartIndex finds the appropriate start index for a specific pod based on lookback time
func findPodStartIndex(counts []*TimestampedCounts, podName string, lookbackSeconds int64) int {
	if len(counts) == 0 {
		return -1
	}

	endTime := counts[len(counts)-1].timestamp
	lookbackTime := endTime - lookbackSeconds

	// Find the earliest index where:
	// 1. The pod exists in the data
	// 2. The timestamp is within the lookback window
	for i := 0; i < len(counts); i++ {
		if counts[i].timestamp < lookbackTime {
			continue
		}

		podTimeSeries := counts[i].PodTimeSeriesSnapshot()
		if _, exists := podTimeSeries[podName]; exists {
			return i
		}
	}

	return -1 // Pod not found within lookback window
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
