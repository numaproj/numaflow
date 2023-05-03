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
func CalculateRate(tc queue.OverflowQueue[TimestampedCount], lookback int64) float64 {
	// TODO - This is a dummy implementation. It should be replaced with a real one.
	_ = tc
	_ = lookback

	_ = tc.Items()[0].timestamp
	_ = tc.Items()[0].count
	return float64(0)
}
