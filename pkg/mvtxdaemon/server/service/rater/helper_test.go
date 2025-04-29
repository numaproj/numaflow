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
	"sync"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"

	"github.com/stretchr/testify/assert"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const TestTime = 1620000000

func TestUpdateCount(t *testing.T) {
	t.Run("givenTimeExistsPodExistsCountAvailable_whenUpdate_thenUpdatePodPartitionCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodMetricsCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime, &PodMetricsCount{"pod1", 20.0})

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podReadCounts["pod1"])
	})

	t.Run("givenTimeExistsPodNotExistsCountAvailable_whenUpdate_thenAddPodCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodMetricsCount{"pod1", 20.0})
		q.Append(tc)

		UpdateCount(q, TestTime, &PodMetricsCount{"pod2", 10.0})

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podReadCounts["pod1"])
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod2"])
	})

	t.Run("givenTimeExistsPodExistsCountNotAvailable_whenUpdate_thenNotUpdatePod", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodMetricsCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime, nil)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 1, len(q.Items()[0].podReadCounts))
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
	})

	t.Run("givenTimeExistsPodNotExistsCountNotAvailable_whenUpdate_thenNoUpdate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodMetricsCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime, nil)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
	})

	t.Run("givenTimeNotExistsCountAvailable_whenUpdate_thenAddNewItem", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodMetricsCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime+1, &PodMetricsCount{"pod1", 20.0})

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
		assert.Equal(t, 20.0, q.Items()[1].podReadCounts["pod1"])
	})

	t.Run("givenTimeNotExistsCountNotAvailable_whenUpdate_thenAddEmptyItem", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodMetricsCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime+1, nil)

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
		assert.Equal(t, 0, len(q.Items()[1].podReadCounts))
	})
}

func TestCalculatePending(t *testing.T) {
	t.Run("givenCollectedTimeLessThanTwo_whenCalculateRate_thenReturnPendingNotAvailable", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		// no data
		assert.Equal(t, isb.PendingNotAvailable, CalculatePending(q, 10))

		// only one data
		now := time.Now()
		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodMetricsCount{"pod0", 5.0})
		q.Append(tc1)
		assert.Equal(t, isb.PendingNotAvailable, CalculatePending(q, 10))
	})

	t.Run("singlePod_givenCountIncreases_whenCalculatePending_thenReturnPending", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20) //80
		tc1.Update(&PodMetricsCount{"pod0", 3.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10) //90
		tc2.Update(&PodMetricsCount{"pod0", 20.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix()) // 100
		tc3.Update(&PodMetricsCount{"pod0", 10.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, isb.PendingNotAvailable, CalculatePending(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, int64(15), CalculatePending(q, 15))
		// tc1 and tc2 are used to calculate the pending
		assert.Equal(t, int64(11), CalculatePending(q, 25))
		// tc1 and tc2 are used to calculate the pending
		assert.Equal(t, int64(11), CalculatePending(q, 100))
	})
}

func TestCalculateRate(t *testing.T) {
	t.Run("givenCollectedTimeLessThanTwo_whenCalculateRate_thenReturnRateNotAvailable", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		// no data
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 10))

		// only one data
		now := time.Now()
		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodMetricsCount{"pod1", 5.0})
		q.Append(tc1)
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 10))
	})

	t.Run("singlePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodMetricsCount{"pod1", 5.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc2.Update(&PodMetricsCount{"pod1", 10.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc3.Update(&PodMetricsCount{"pod1", 20.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 0.5, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 0.5, CalculateRate(q, 100))
	})

	t.Run("singlePod_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodMetricsCount{"pod1", 200.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodMetricsCount{"pod1", 100.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodMetricsCount{"pod1", 50.0})
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc4.Update(&PodMetricsCount{"pod1", 80.0})
		q.Append(tc4)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15))
		// tc2 and tc3 are used to calculate the rate
		assert.Equal(t, 5.0, CalculateRate(q, 25))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 7.5, CalculateRate(q, 35))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 7.5, CalculateRate(q, 100))
	})

	t.Run("multiplePods_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodMetricsCount{"pod1", 50.0})
		tc1.Update(&PodMetricsCount{"pod2", 100.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodMetricsCount{"pod1", 100.0})
		tc2.Update(&PodMetricsCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodMetricsCount{"pod1", 200.0})
		tc3.Update(&PodMetricsCount{"pod2", 300.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 15.0, CalculateRate(q, 35))
	})

	t.Run("multiplePods_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodMetricsCount{"pod1", 200.0})
		tc1.Update(&PodMetricsCount{"pod2", 300.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodMetricsCount{"pod1", 100.0})
		tc2.Update(&PodMetricsCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodMetricsCount{"pod1", 50.0})
		tc3.Update(&PodMetricsCount{"pod2", 100.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 30.0, CalculateRate(q, 35))
	})

	t.Run("multiplePods_givenOnePodRestarts_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodMetricsCount{"pod1", 50.0})
		tc1.Update(&PodMetricsCount{"pod2", 300.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodMetricsCount{"pod1", 100.0})
		tc2.Update(&PodMetricsCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodMetricsCount{"pod1", 200.0})
		tc3.Update(&PodMetricsCount{"pod2", 100.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 25.0, CalculateRate(q, 35))
	})

	t.Run("multiplePods_givenPodsComeAndGo_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 30)
		tc1.Update(&PodMetricsCount{"pod1", 200.0})
		tc1.Update(&PodMetricsCount{"pod2", 90.0})
		tc1.Update(&PodMetricsCount{"pod3", 50.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc2.Update(&PodMetricsCount{"pod1", 100.0})
		tc2.Update(&PodMetricsCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodMetricsCount{"pod1", 50.0})
		tc3.Update(&PodMetricsCount{"pod2", 300.0})
		tc3.Update(&PodMetricsCount{"pod4", 100.0})
		q.Append(tc3)

		tc4 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc4.Update(&PodMetricsCount{"pod2", 400.0})
		tc4.Update(&PodMetricsCount{"pod3", 200.0})
		tc4.Update(&PodMetricsCount{"pod100", 200.0})
		q.Append(tc4)

		// vertex rate
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5))
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15))
		assert.Equal(t, 25.0, CalculateRate(q, 25))
		assert.Equal(t, 23.0, CalculateRate(q, 35))
		assert.Equal(t, 23.0, CalculateRate(q, 100))
	})
}

// Helper function to create a TimestampedCounts instance
func newTimestampedCounts(timestamp int64, counts map[string]float64) *TimestampedCounts {
	return &TimestampedCounts{
		timestamp:     timestamp,
		podReadCounts: counts,
		lock:          new(sync.RWMutex),
	}
}

// TestCalculateMaxLookback tests various scenarios on the CalculateMaxLookback function
func TestCalculateMaxLookback(t *testing.T) {
	tests := []struct {
		name        string
		counts      []*TimestampedCounts
		startIndex  int
		endIndex    int
		expectedMax int64
	}{
		{
			name: "Uniform data across the range",
			counts: []*TimestampedCounts{
				newTimestampedCounts(100, map[string]float64{"pod1": 100, "pod2": 200}),
				newTimestampedCounts(200, map[string]float64{"pod1": 100, "pod2": 200}),
				newTimestampedCounts(400, map[string]float64{"pod1": 100, "pod2": 200}),
			},
			startIndex:  0,
			endIndex:    2,
			expectedMax: 300,
		},
		{
			name: "Values change midway",
			counts: []*TimestampedCounts{
				newTimestampedCounts(100, map[string]float64{"pod1": 100, "pod2": 150}),
				newTimestampedCounts(240, map[string]float64{"pod1": 100, "pod2": 200}),
				newTimestampedCounts(360, map[string]float64{"pod1": 150, "pod2": 200}),
			},
			startIndex:  0,
			endIndex:    2,
			expectedMax: 260,
		},
		{
			name: "No data change across any pods",
			counts: []*TimestampedCounts{
				newTimestampedCounts(100, map[string]float64{"pod1": 500}),
				newTimestampedCounts(600, map[string]float64{"pod1": 500}),
			},
			startIndex:  0,
			endIndex:    1,
			expectedMax: 500, // Entire duration
		},
		{
			name: "Edge Case: One entry only",
			counts: []*TimestampedCounts{
				newTimestampedCounts(100, map[string]float64{"pod1": 100}),
			},
			startIndex:  0,
			endIndex:    0,
			expectedMax: 0, // No duration difference
		},
		{
			name: "Rapid changes in sequential entries",
			counts: []*TimestampedCounts{
				newTimestampedCounts(100, map[string]float64{"pod1": 500, "pod2": 400}),
				newTimestampedCounts(130, map[string]float64{"pod1": 600, "pod2": 400}),
				newTimestampedCounts(160, map[string]float64{"pod1": 600, "pod2": 600}),
			},
			startIndex:  0,
			endIndex:    2,
			expectedMax: 60,
		},
		{
			// Here the pod has an initial read count, and then would we see a pod count as 0.
			// This is equated as a refresh in counts, and thus
			name: "Pod goes to zero",
			counts: []*TimestampedCounts{
				newTimestampedCounts(0, map[string]float64{"pod1": 50}), // Initial count
				newTimestampedCounts(30, map[string]float64{"pod1": 50}),
				newTimestampedCounts(60, map[string]float64{"pod1": 0}),   // Count falls to zero
				newTimestampedCounts(120, map[string]float64{"pod1": 25}), // Count returns
				newTimestampedCounts(180, map[string]float64{"pod1": 25}), // Count stays stable
				newTimestampedCounts(240, map[string]float64{"pod1": 25}), // Count stays stable again
			},
			startIndex:  0,
			endIndex:    5,
			expectedMax: 120, // from index 3,5
		},
		{
			name: "Pod goes to zero - 2",
			counts: []*TimestampedCounts{
				newTimestampedCounts(0, map[string]float64{"pod1": 60}),
				newTimestampedCounts(60, map[string]float64{"pod1": 60}),
				newTimestampedCounts(120, map[string]float64{"pod1": 70}),
				newTimestampedCounts(180, map[string]float64{"pod1": 0}),
				newTimestampedCounts(240, map[string]float64{"pod1": 25}),
				newTimestampedCounts(300, map[string]float64{"pod1": 25}),
			},
			startIndex:  0,
			endIndex:    5,
			expectedMax: 120, // here idx 0,2 should be used, after going to zero it resets
		},
		{
			// this is a case where one pod never got any data which we consider as read count = 0 always
			// in such a case we should not use this pod for calculation
			name: "One pod no data, other >0 ",
			counts: []*TimestampedCounts{
				newTimestampedCounts(0, map[string]float64{"pod1": 0, "pod2": 5}),
				newTimestampedCounts(60, map[string]float64{"pod1": 0, "pod2": 5}),
				newTimestampedCounts(120, map[string]float64{"pod1": 0, "pod2": 5}),
				newTimestampedCounts(180, map[string]float64{"pod1": 0, "pod2": 5}),
				newTimestampedCounts(240, map[string]float64{"pod1": 0, "pod2": 6}),
				newTimestampedCounts(300, map[string]float64{"pod1": 0, "pod2": 6}),
			},
			startIndex:  0,
			endIndex:    5,
			expectedMax: 240,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxDuration := CalculateMaxLookback(tt.counts, tt.startIndex, tt.endIndex)
			assert.Equal(t, tt.expectedMax, maxDuration)
		})
	}
}
