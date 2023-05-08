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

	"github.com/numaproj/numaflow/pkg/shared/queue"
)

// TimestampedCount is a helper struct to wrap a count number and timestamp pair
type TimestampedCount struct {
	// count is the number of messages processed
	count int64
	// timestamp in seconds, is the time when the count is recorded
	timestamp int64
}

// CalculateRate calculates the processing rate over the last lookback seconds.
// It uses the timestamped count queue to retrieve the first and last count in the lookback window and calculate the rate using formula:
// rate = (lastCount - firstCount) / (lastTimestamp - firstTimestamp)
func CalculateRate(tc *queue.OverflowQueue[TimestampedCount], lookback int64) float64 {
	if tc.Length() < 2 {
		return 0
	}
	counts := tc.Items()
	endCountInfo := counts[len(counts)-1]
	startCountInfo := counts[len(counts)-2]
	now := time.Now().Unix()
	if now-startCountInfo.timestamp > lookback {
		return 0
	}
	for i := len(counts) - 3; i >= 0; i-- {
		if now-counts[i].timestamp <= lookback {
			startCountInfo = counts[i]
		} else {
			break
		}
	}
	return float64(endCountInfo.count-startCountInfo.count) / float64(endCountInfo.timestamp-startCountInfo.timestamp)
}

// UpdateCountTrackers updates the count trackers using the latest count numbers podTotalCounts.
// it updates the lastSawPodCounts and timestampedTotalCounts to reflect the latest counts.
func UpdateCountTrackers(tc *queue.OverflowQueue[TimestampedCount], lastSawPodCounts, podTotalCounts map[string]float64) {
	delta := 0.0
	for podName, newCount := range podTotalCounts {
		lastCount, exists := lastSawPodCounts[podName]
		if newCount == CountNotAvailable {
			// this is the case where the count is not available for this particular pod.
			// we assume that it indicates the pod is not running, hence we skip counting this pod.
			lastSawPodCounts[podName] = newCount
			continue
		}

		if exists && lastCount != CountNotAvailable && newCount >= lastCount {
			// this is the normal case where the count increases.
			delta += newCount - lastCount
		} else {
			// if the pod doesn't exist in last check, or the count decreases,
			// or for certain reasons last saw count is CountNotAvailable,
			// we assume that the pod either was restarted or a new pod just coming up.
			delta += newCount
		}
		lastSawPodCounts[podName] = newCount
	}

	lastSawTotalCount := 0.0
	if tc.Length() > 0 {
		lastSawTotalCount = float64(tc.Newest().count)
	}

	newTotalCount := lastSawTotalCount + delta
	tc.Append(TimestampedCount{count: int64(newTotalCount), timestamp: time.Now().Unix()})
}
