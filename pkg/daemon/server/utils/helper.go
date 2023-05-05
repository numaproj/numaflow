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

// TODO - rename this file

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
// It updates the lastSawPodCounts and timestampedTotalCounts to reflect the latest counts.
func UpdateCountTrackers(tc *queue.OverflowQueue[TimestampedCount], lastSawPodCounts, podTotalCounts map[string]float64) {
	// This is a simple implementation. It assumes no pod crashes and restarts.
	// TODO - replace it with a new one that aligns with the design doc.
	totalCount := 0
	for _, count := range podTotalCounts {
		if count != CountNotAvailable {
			totalCount += int(count)
		}
	}
	tc.Append(TimestampedCount{count: int64(totalCount), timestamp: time.Now().Unix()})
	_ = lastSawPodCounts
}
