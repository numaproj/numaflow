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

func TestUpdateCount(t *testing.T) {
	t.Run("givenTimeExistsPodExistsCountAvailable_whenUpdate_thenUpdatePodPartitionCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime, &PodReadCount{"pod1", 20.0})

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podReadCounts["pod1"])
	})

	t.Run("givenTimeExistsPodNotExistsCountAvailable_whenUpdate_thenAddPodCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", 20.0})
		q.Append(tc)

		UpdateCount(q, TestTime, &PodReadCount{"pod2", 10.0})

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podReadCounts["pod1"])
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod2"])
	})

	t.Run("givenTimeExistsPodExistsCountNotAvailable_whenUpdate_thenNotUpdatePod", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime, nil)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 1, len(q.Items()[0].podReadCounts))
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
	})

	t.Run("givenTimeExistsPodNotExistsCountNotAvailable_whenUpdate_thenNoUpdate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime, nil)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
	})

	t.Run("givenTimeNotExistsCountAvailable_whenUpdate_thenAddNewItem", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime+1, &PodReadCount{"pod1", 20.0})

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
		assert.Equal(t, 20.0, q.Items()[1].podReadCounts["pod1"])
	})

	t.Run("givenTimeNotExistsCountNotAvailable_whenUpdate_thenAddEmptyItem", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", 10.0})
		q.Append(tc)

		UpdateCount(q, TestTime+1, nil)

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podReadCounts["pod1"])
		assert.Equal(t, 0, len(q.Items()[1].podReadCounts))
	})
}

func TestCalculateRate(t *testing.T) {
	t.Run("givenCollectedTimeLessThanTwo_whenCalculateRate_thenReturnZero", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		// no data
		assert.Equal(t, 0.0, CalculateRate(q, 10))

		// only one data
		now := time.Now()
		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodReadCount{"pod1", 5.0})
		q.Append(tc1)
		assert.Equal(t, 0.0, CalculateRate(q, 10))
	})

	t.Run("singlePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodReadCount{"pod1", 5.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc2.Update(&PodReadCount{"pod1", 10.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc3.Update(&PodReadCount{"pod1", 20.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 15))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 0.5, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 0.5, CalculateRate(q, 100))
	})

	t.Run("singlePod_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", 200.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", 100.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", 50.0})
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc4.Update(&PodReadCount{"pod1", 80.0})
		q.Append(tc4)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 15))
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
		tc1.Update(&PodReadCount{"pod1", 50.0})
		tc1.Update(&PodReadCount{"pod2", 100.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", 100.0})
		tc2.Update(&PodReadCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", 200.0})
		tc3.Update(&PodReadCount{"pod2", 300.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 15))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 15.0, CalculateRate(q, 35))
	})

	t.Run("multiplePods_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", 200.0})
		tc1.Update(&PodReadCount{"pod2", 300.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", 100.0})
		tc2.Update(&PodReadCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", 50.0})
		tc3.Update(&PodReadCount{"pod2", 100.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 15))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 30.0, CalculateRate(q, 35))
	})

	t.Run("multiplePods_givenOnePodRestarts_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", 50.0})
		tc1.Update(&PodReadCount{"pod2", 300.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", 100.0})
		tc2.Update(&PodReadCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", 200.0})
		tc3.Update(&PodReadCount{"pod2", 100.0})
		q.Append(tc3)

		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 15))
		// no enough data collected within lookback seconds, expect rate 0
		assert.Equal(t, 0.0, CalculateRate(q, 25))
		// tc1 and tc2 are used to calculate the rate
		assert.Equal(t, 25.0, CalculateRate(q, 35))
	})

	t.Run("multiplePods_givenPodsComeAndGo_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", 200.0})
		tc1.Update(&PodReadCount{"pod2", 90.0})
		tc1.Update(&PodReadCount{"pod3", 50.0})
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", 100.0})
		tc2.Update(&PodReadCount{"pod2", 200.0})
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", 50.0})
		tc3.Update(&PodReadCount{"pod2", 300.0})
		tc3.Update(&PodReadCount{"pod4", 100.0})
		q.Append(tc3)

		tc4 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc4.Update(&PodReadCount{"pod2", 400.0})
		tc4.Update(&PodReadCount{"pod3", 200.0})
		tc4.Update(&PodReadCount{"pod100", 200.0})
		q.Append(tc4)

		// vertex rate
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 0.0, CalculateRate(q, 15))
		assert.Equal(t, 25.0, CalculateRate(q, 25))
		assert.Equal(t, 23.0, CalculateRate(q, 35))
		assert.Equal(t, 23.0, CalculateRate(q, 100))
	})
}
