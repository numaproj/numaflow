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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const TestTime = 1620000000

func TestUpdateCount(t *testing.T) {
	t.Run("givenTimeExistsPodExistsPartitionExistsCountAvailable_whenUpdate_thenUpdatePodPartitionCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc)

		UpdateCount(q, TestTime, &PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podPartitionCount["pod1"]["partition1"])
	})

	t.Run("givenTimeExistsPodExistsPartitionNotExistsCountAvailable_whenUpdate_thenAddPodPartitionCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc)

		UpdateCount(q, TestTime, &PodReadCount{"pod1", map[string]float64{"partition1": 20.0, "partition2": 30.0}})

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podPartitionCount["pod1"]["partition1"])
		assert.Equal(t, 30.0, q.Items()[0].podPartitionCount["pod1"]["partition2"])
	})

	t.Run("givenTimeExistsPodNotExistsCountAvailable_whenUpdate_thenAddPodCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})
		q.Append(tc)

		UpdateCount(q, TestTime, &PodReadCount{"pod2", map[string]float64{"partition1": 10.0}})

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podPartitionCount["pod1"]["partition1"])
		assert.Equal(t, 10.0, q.Items()[0].podPartitionCount["pod2"]["partition1"])
	})

	t.Run("givenTimeExistsPodExistsCountNotAvailable_whenUpdate_thenNotUpdatePod", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc)

		UpdateCount(q, TestTime, nil)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 1, len(q.Items()[0].podPartitionCount))
		assert.Equal(t, 10.0, q.Items()[0].podPartitionCount["pod1"]["partition1"])
	})

	t.Run("givenTimeExistsPodNotExistsCountNotAvailable_whenUpdate_thenNoUpdate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc)

		UpdateCount(q, TestTime, nil)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podPartitionCount["pod1"]["partition1"])
	})

	t.Run("givenTimeNotExistsCountAvailable_whenUpdate_thenUpdateNewTimeWithPodAndCloseWindowForPrevTime", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc)

		UpdateCount(q, TestTime+1, &PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podPartitionCount["pod1"]["partition1"])
		assert.Equal(t, 20.0, q.Items()[1].podPartitionCount["pod1"]["partition1"])
		assert.Equal(t, true, tc.IsWindowClosed())
		assert.Equal(t, map[string]map[string]float64{"pod1": {"partition1": 10.0}}, tc.PodDeltaCountSnapshot())
	})

	t.Run("givenTimeNotExistsCountNotAvailable_whenUpdate_thenAddEmptyItem", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc)

		UpdateCount(q, TestTime+1, nil)

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podPartitionCount["pod1"]["partition1"])
		assert.Equal(t, 0, len(q.Items()[1].podPartitionCount))
	})
}

func TestCalculateRate(t *testing.T) {
	t.Run("givenCollectedTimeLessThanTwo_whenCalculateRate_thenReturnZero", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		// no data
		assert.Equal(t, 0.0, CalculateRate(q, 10, "partition1"))

		// only one data
		now := time.Now()
		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 5.0}})
		q.Append(tc1)
		assert.Equal(t, 0.0, CalculateRate(q, 10, "partition1"))
	})

	t.Run("singlePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 5.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})
		q.Append(tc3)
		tc3.CloseWindow(tc2)

		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 1.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 0.75, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 0.75, CalculateRate(q, 100, "partition1"))
	})

	t.Run("singlePod_givenCountIncreases_whenCalculateRate_thenReturnRate_excludeOpenWindow", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 5.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})
		q.Append(tc3)

		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 0.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 0.5, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 0.5, CalculateRate(q, 100, "partition1"))
	})

	t.Run("singlePod_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		q.Append(tc3)
		tc3.CloseWindow(tc2)
		tc4 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc4.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 80.0}})
		q.Append(tc4)
		tc4.CloseWindow(tc3)

		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 3.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 4.0, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 6.0, CalculateRate(q, 35, "partition1"))
		assert.Equal(t, 6.0, CalculateRate(q, 100, "partition1"))
	})

	t.Run("singlePod_givenCountDecreases_whenCalculateRate_thenReturnRate_excludeOpenWindow", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		q.Append(tc3)
		tc3.CloseWindow(tc2)
		tc4 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc4.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 80.0}})
		q.Append(tc4)

		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 0.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 5.0, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 7.5, CalculateRate(q, 35, "partition1"))
		assert.Equal(t, 7.5, CalculateRate(q, 100, "partition1"))
	})

	t.Run("multiplePods_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 100.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 200.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 300.0}})
		q.Append(tc3)
		tc3.CloseWindow(tc2)

		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 0.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 20.0, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 17.5, CalculateRate(q, 35, "partition1"))
	})

	t.Run("multiplePods_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 300.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 200.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 100.0}})
		q.Append(tc3)
		tc3.CloseWindow(tc2)

		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 0.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 15.0, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 22.5, CalculateRate(q, 35, "partition1"))
	})

	t.Run("multiplePods_givenOnePodRestarts_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 300.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 200.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 100.0}})
		q.Append(tc3)
		tc3.CloseWindow(tc2)

		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 0.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 20.0, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 22.5, CalculateRate(q, 35, "partition1"))
	})

	t.Run("multiplePods_givenPodsComeAndGo_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 30)
		tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
		tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 90.0}})
		tc1.Update(&PodReadCount{"pod3", map[string]float64{"partition3": 50.0}})
		q.Append(tc1)
		tc1.CloseWindow(nil)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
		tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 200.0}})
		q.Append(tc2)
		tc2.CloseWindow(tc1)
		tc3 := NewTimestampedCounts(now.Truncate(CountWindow).Unix() - 10)
		tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 50.0}})
		tc3.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 300.0}})
		tc3.Update(&PodReadCount{"pod4", map[string]float64{"partition4": 100.0}})
		q.Append(tc3)
		tc3.CloseWindow(tc2)
		tc4 := NewTimestampedCounts(now.Truncate(CountWindow).Unix())
		tc4.Update(&PodReadCount{"pod2", map[string]float64{"partition2": 400.0}})
		tc4.Update(&PodReadCount{"pod3", map[string]float64{"partition3": 200.0}})
		tc4.Update(&PodReadCount{"pod100", map[string]float64{"partition100": 200.0}})
		q.Append(tc4)
		tc4.CloseWindow(tc3)

		// partition1 rate
		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition1"))
		assert.Equal(t, 0.0, CalculateRate(q, 15, "partition1"))
		assert.Equal(t, 2.5, CalculateRate(q, 25, "partition1"))
		assert.Equal(t, 5.0, CalculateRate(q, 35, "partition1"))
		assert.Equal(t, 5.0, CalculateRate(q, 100, "partition1"))

		// partition2 rate
		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition2"))
		assert.Equal(t, 10.0, CalculateRate(q, 15, "partition2"))
		assert.Equal(t, 10.0, CalculateRate(q, 25, "partition2"))
		assert.InDelta(t, 10.333, CalculateRate(q, 35, "partition2"), 0.001)
		assert.InDelta(t, 10.333, CalculateRate(q, 100, "partition2"), 0.001)

		// partition3 rate
		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition3"))
		assert.Equal(t, 20.0, CalculateRate(q, 15, "partition3"))
		assert.Equal(t, 10.0, CalculateRate(q, 25, "partition3"))
		assert.InDelta(t, 6.666, CalculateRate(q, 100, "partition3"), 0.001)
		assert.InDelta(t, 6.666, CalculateRate(q, 100, "partition3"), 0.001)

		// partition4 rate
		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition4"))
		assert.Equal(t, 0.0, CalculateRate(q, 15, "partition4"))
		assert.Equal(t, 5.0, CalculateRate(q, 25, "partition4"))
		assert.InDelta(t, 3.333, CalculateRate(q, 35, "partition4"), 0.001)
		assert.InDelta(t, 3.333, CalculateRate(q, 100, "partition4"), 0.001)

		// partition100 rate
		assert.Equal(t, 0.0, CalculateRate(q, 5, "partition100"))
		assert.Equal(t, 20.0, CalculateRate(q, 15, "partition100"))
		assert.Equal(t, 10.0, CalculateRate(q, 25, "partition100"))
		assert.InDelta(t, 6.666, CalculateRate(q, 35, "partition100"), 0.001)
		assert.InDelta(t, 6.666, CalculateRate(q, 100, "partition100"), 0.001)
	})

}
