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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const TestTime = 1620000000

func TestCalculateRate(t *testing.T) {
	t.Run("givenCollectedTimeLessThanTwo_whenCalculateRate_thenReturnRateNotAvailable", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		// no data
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 10, "partition1"))

		// only one data
		now := time.Now()
		tc1 := NewTimestampedCounts(now.Unix() - 20)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 5.0}})
		q.Append(tc1)
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 10, "partition1"))
	})

	t.Run("singlePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Unix() - 20)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 5.0}})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Unix() - 10)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Unix())
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition1"))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 0.5, CalculateRate(q, 25, "partition1"))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 0.5, CalculateRate(q, 100, "partition1"))
	})

	t.Run("singlePod_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Unix())
		tc4.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 80.0}})
		q.Append(tc4)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition1"))
		// tc2 and tc3 are used to calculate the rate
		assert.Equal(t, 5.0, CalculateRate(q, 25, "partition1"))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 7.5, CalculateRate(q, 35, "partition1"))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 7.5, CalculateRate(q, 100, "partition1"))
	})

	t.Run("multiplePods_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 100.0}})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 200.0}})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 300.0}})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 25, "partition1"))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 15.0, CalculateRate(q, 35, "partition1"))
	})

	t.Run("multiplePods_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 300.0}})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 200.0}})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 100.0}})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 25, "partition1"))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 30.0, CalculateRate(q, 35, "partition1"))
	})

	t.Run("multiplePods_givenOnePodRestarts_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 300.0}})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 200.0}})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 100.0}})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 25, "partition1"))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 25.0, CalculateRate(q, 35, "partition1"))
	})

	t.Run("multiplePods_givenPodsComeAndGo_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 90.0}})
		tc1.Update(&PodReadCount{"pod3", map[string]float64{"partition3": 50.0}})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 200.0}})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 300.0}})
		tc3.Update(&PodReadCount{"pod4", map[string]float64{"partition4": 100.0}})
		tc3.Update(&PodReadCount{"pod3", map[string]float64{"partition3": 200.0}})
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Unix())
		tc4.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 400.0}})
		tc4.Update(&PodReadCount{"pod100", map[string]float64{"partition100": 200.0}})
		q.Append(tc4)

		// partition1 rate
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition1"))
		// tc2 and tc3 are used to calculate the rate
		assert.Equal(t, 5.0, CalculateRate(q, 25, "partition1"))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 7.5, CalculateRate(q, 35, "partition1"))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 7.5, CalculateRate(q, 100, "partition1"))

		// partition2 rate
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition2"))
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition2"))
		assert.Equal(t, 10.0, CalculateRate(q, 25, "partition2"))
		assert.Equal(t, 10.5, CalculateRate(q, 35, "partition2"))
		assert.Equal(t, 10.5, CalculateRate(q, 100, "partition2"))

		// partition3 rate
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition3"))
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition3"))
		assert.Equal(t, 20.0, CalculateRate(q, 25, "partition3"))
		assert.Equal(t, 10.0, CalculateRate(q, 35, "partition3"))
		assert.Equal(t, 10.0, CalculateRate(q, 100, "partition3"))

		// partition4 rate
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition4"))
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition4"))
		assert.Equal(t, 10.0, CalculateRate(q, 25, "partition4"))
		assert.Equal(t, 5.0, CalculateRate(q, 35, "partition4"))
		assert.Equal(t, 5.0, CalculateRate(q, 100, "partition4"))

		// partition100 rate
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition100"))
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition100"))
		assert.Equal(t, 0.0, CalculateRate(q, 25, "partition100"))
		assert.Equal(t, 0.0, CalculateRate(q, 35, "partition100"))
		assert.Equal(t, 0.0, CalculateRate(q, 100, "partition100"))
	})

	t.Run("multiplePods_givenOnePodHandleMultiplePartitions_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now().Unix()

		// this test uses an extreme case where pod1 handle3 10 messages at a time for each partition, and pod 2 100, pod 3 1000
		tc1 := NewTimestampedCounts(now - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0, "partition2": 20.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 10.0, "partition2": 20.0}})
		tc1.Update(&PodReadCount{"pod3", map[string]float64{"partition1": 10.0, "partition2": 20.0}})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 20.0, "partition2": 30.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 110.0, "partition2": 120.0}})
		tc2.Update(&PodReadCount{"pod3", map[string]float64{"partition1": 1010.0, "partition2": 1020.0}})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 30.0, "partition2": 40.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 210.0, "partition2": 220.0}})
		tc3.Update(&PodReadCount{"pod3", map[string]float64{"partition1": 2010.0, "partition2": 2020.0}})
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now)
		tc4.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 40.0, "partition2": 50.0}})
		tc4.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 310.0, "partition2": 320.0}})
		tc4.Update(&PodReadCount{"pod3", map[string]float64{"partition1": 3010.0, "partition2": 3020.0}})
		q.Append(tc4)

		// partition1 rate
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition1"))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition1"))
		// tc2 and tc3 are used to calculate the rate
		assert.Equal(t, 111.0, CalculateRate(q, 25, "partition1"))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 111.0, CalculateRate(q, 35, "partition1"))
		// tc1, 2 and 3 are used to calculate the rate
		assert.Equal(t, 111.0, CalculateRate(q, 100, "partition1"))

		// partition2 rate
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 5, "partition2"))
		assert.Equal(t, rateNotAvailable, CalculateRate(q, 15, "partition2"))
		assert.Equal(t, 111.0, CalculateRate(q, 25, "partition2"))
		assert.Equal(t, 111.0, CalculateRate(q, 35, "partition2"))
		assert.Equal(t, 111.0, CalculateRate(q, 100, "partition2"))
	})
}
