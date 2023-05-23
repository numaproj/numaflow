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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const TestTime = 1620000000

func TestUpdateCount(t *testing.T) {
	t.Run("givenTimeExistsPodExistsCountAvailable_whenUpdate_thenUpdatePodCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update("pod1", 10.0)
		q.Append(tc)

		UpdateCount(q, TestTime, "pod1", 20.0)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podCounts["pod1"])
	})

	t.Run("givenTimeExistsPodNotExistsCountAvailable_whenUpdate_thenAddPodCount", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update("pod1", 20.0)
		q.Append(tc)

		UpdateCount(q, TestTime, "pod2", 10.0)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 20.0, q.Items()[0].podCounts["pod1"])
		assert.Equal(t, 10.0, q.Items()[0].podCounts["pod2"])
	})

	t.Run("givenTimeExistsPodExistsCountNotAvailable_whenUpdate_thenRemovePod", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update("pod1", 10.0)
		q.Append(tc)

		UpdateCount(q, TestTime, "pod1", CountNotAvailable)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 0, len(q.Items()[0].podCounts))
	})

	t.Run("givenTimeExistsPodNotExistsCountNotAvailable_whenUpdate_thenNoUpdate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update("pod1", 10.0)
		q.Append(tc)

		UpdateCount(q, TestTime, "pod2", CountNotAvailable)

		assert.Equal(t, 1, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podCounts["pod1"])
	})

	t.Run("givenTimeNotExistsCountAvailable_whenUpdate_thenUpdateNewTimeWithPod", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update("pod1", 10.0)
		q.Append(tc)

		UpdateCount(q, TestTime+1, "pod1", 20.0)

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podCounts["pod1"])
		assert.Equal(t, 20.0, q.Items()[1].podCounts["pod1"])
	})

	t.Run("givenTimeNotExistsCountNotAvailable_whenUpdate_thenAddEmptyItem", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		tc := NewTimestampedCounts(TestTime)
		tc.Update("pod1", 10.0)
		q.Append(tc)

		UpdateCount(q, TestTime+1, "pod2", CountNotAvailable)

		assert.Equal(t, 2, q.Length())
		assert.Equal(t, 10.0, q.Items()[0].podCounts["pod1"])
		assert.Equal(t, 0, len(q.Items()[1].podCounts))
	})
}

func TestCalculateRate(t *testing.T) {
	t.Run("givenCollectedTimeLessThanTwo_whenCalculateRate_thenReturnZero", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		rate := CalculateRate(q, 10)
		assert.Equal(t, 0.0, rate)
	})

	t.Run("singlePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc1.Update("pod1", 5.0)
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 10)
		tc2.Update("pod1", 10.0)
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(time.Second * 10).Unix())
		tc3.Update("pod1", 20.0)
		q.Append(tc3)

		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 1.0, CalculateRate(q, 15))
		assert.Equal(t, 0.75, CalculateRate(q, 25))
		assert.Equal(t, 0.75, CalculateRate(q, 100))
	})

	t.Run("singlePod_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 30)
		tc1.Update("pod1", 200.0)
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc2.Update("pod1", 100.0)
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 10)
		tc3.Update("pod1", 50.0)
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Truncate(time.Second * 10).Unix())
		tc4.Update("pod1", 80.0)
		q.Append(tc4)

		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 3.0, CalculateRate(q, 15))
		assert.Equal(t, 4.0, CalculateRate(q, 25))
		assert.Equal(t, 6.0, CalculateRate(q, 35))
		assert.Equal(t, 6.0, CalculateRate(q, 100))
	})

	t.Run("multiplePods_givenCountIncreasesAndDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 30)
		tc1.Update("pod1", 200.0)
		tc1.Update("pod2", 100.0)
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc2.Update("pod1", 100.0)
		tc2.Update("pod2", 200.0)
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 10)
		tc3.Update("pod1", 50.0)
		tc3.Update("pod2", 300.0)
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Truncate(time.Second * 10).Unix())
		tc4.Update("pod1", 80.0)
		tc4.Update("pod2", 400.0)
		q.Append(tc4)

		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 13.0, CalculateRate(q, 15))
		assert.Equal(t, 14.0, CalculateRate(q, 25))
		assert.Equal(t, 16.0, CalculateRate(q, 35))
		assert.Equal(t, 16.0, CalculateRate(q, 100))
	})

	t.Run("multiplePods_givenPodsComeAndGo_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](1800)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 30)
		tc1.Update("pod1", 200.0)
		tc1.Update("pod2", 90.0)
		tc1.Update("pod3", 50.0)
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc2.Update("pod1", 100.0)
		tc2.Update("pod2", 200.0)
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 10)
		tc3.Update("pod1", 50.0)
		tc3.Update("pod2", 300.0)
		tc3.Update("pod4", 100.0)
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Truncate(time.Second * 10).Unix())
		tc4.Update("pod2", 400.0)
		tc4.Update("pod3", 200.0)
		tc4.Update("pod100", 200.0)
		q.Append(tc4)

		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 50.0, CalculateRate(q, 15))
		assert.Equal(t, 37.5, CalculateRate(q, 25))
		assert.Equal(t, 32.0, CalculateRate(q, 35))
		assert.Equal(t, 32.0, CalculateRate(q, 100))
	})

	t.Run("queueOverflowed_SinglePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[*TimestampedCounts](3)
		now := time.Now()

		tc1 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 30)
		tc1.Update("pod1", 200.0)
		tc1.Update("pod2", 90.0)
		tc1.Update("pod3", 50.0)
		q.Append(tc1)
		tc2 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 20)
		tc2.Update("pod1", 100.0)
		tc2.Update("pod2", 200.0)
		q.Append(tc2)
		tc3 := NewTimestampedCounts(now.Truncate(time.Second*10).Unix() - 10)
		tc3.Update("pod1", 50.0)
		tc3.Update("pod2", 300.0)
		tc3.Update("pod4", 100.0)
		q.Append(tc3)
		tc4 := NewTimestampedCounts(now.Truncate(time.Second * 10).Unix())
		tc4.Update("pod2", 400.0)
		tc4.Update("pod3", 200.0)
		tc4.Update("pod100", 200.0)
		q.Append(tc4)

		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 50.0, CalculateRate(q, 15))
		assert.Equal(t, 37.5, CalculateRate(q, 25))
		assert.Equal(t, 37.5, CalculateRate(q, 35))
		assert.Equal(t, 37.5, CalculateRate(q, 100))
	})
}
