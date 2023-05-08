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

func TestCalculateRate(t *testing.T) {
	testCases := []struct {
		name              string
		timestampedCounts []TimestampedCount
		lookback          int64
		expectedRate      float64
	}{
		{
			name:              "empty queue",
			timestampedCounts: []TimestampedCount{},
			lookback:          60,
			expectedRate:      0,
		},
		{
			name: "single count in queue",
			timestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 5},
			},
			lookback:     60,
			expectedRate: 0,
		},
		{
			name: "all counts outside lookback",
			timestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 120, count: 3},
				{timestamp: time.Now().Unix() - 100, count: 5},
				{timestamp: time.Now().Unix() - 80, count: 8},
			},
			lookback:     60,
			expectedRate: 0,
		},
		{
			name: "lookback is zero",
			timestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 40, count: 5},
				{timestamp: time.Now().Unix() - 10, count: 10},
			},
			lookback:     0,
			expectedRate: 0,
		},
		{
			name: "two counts in queue within lookback",
			timestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 40, count: 5},
				{timestamp: time.Now().Unix() - 10, count: 10},
			},
			lookback:     60,
			expectedRate: 0.166,
		},
		{
			name: "multiple counts in queue, some outside lookback",
			timestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 100, count: 3},
				{timestamp: time.Now().Unix() - 70, count: 5},
				{timestamp: time.Now().Unix() - 40, count: 8},
				{timestamp: time.Now().Unix() - 10, count: 12},
			},
			lookback:     60,
			expectedRate: 0.133,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcQueue := sharedqueue.New[TimestampedCount](1800)
			for _, item := range tc.timestampedCounts {
				tcQueue.Append(item)
			}
			actualRate := CalculateRate(tcQueue, tc.lookback)
			assert.InDelta(t, tc.expectedRate, actualRate, 0.001)
		})
	}
}

func TestUpdateCountTrackers(t *testing.T) {
	testCases := []struct {
		name                     string
		initialTimestampedCounts []TimestampedCount
		initialLastSawCounts     map[string]float64
		podTotalCounts           map[string]float64
		expectedDelta            float64
	}{
		{
			name: "normal case",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 5},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": 2,
				"pod2": 3,
			},
			podTotalCounts: map[string]float64{
				"pod1": 4,
				"pod2": 4,
			},
			expectedDelta: 3,
		},
		{
			name: "podTotalCounts count not available",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 5},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": 2,
				"pod2": 3,
			},
			podTotalCounts: map[string]float64{
				"pod1": CountNotAvailable,
				"pod2": 4,
			},
			expectedDelta: 1,
		},
		{
			name: "initialLastSawCounts count not available",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 5},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": CountNotAvailable,
				"pod2": 3,
			},
			podTotalCounts: map[string]float64{
				"pod1": 2,
				"pod2": 4,
			},
			expectedDelta: 3,
		},
		{
			name: "pod count decrease",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 5},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": 2,
				"pod2": 3,
			},
			podTotalCounts: map[string]float64{
				"pod1": 1,
				"pod2": 4,
			},
			expectedDelta: 2,
		},
		{
			name: "new pod in podTotalCounts",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 5},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": 2,
			},
			podTotalCounts: map[string]float64{
				"pod1": 3,
				"pod2": 2,
			},
			expectedDelta: 3,
		},
		{
			name: "all counts lower than lastSawPodCounts",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 10},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": 6,
				"pod2": 4,
			},
			podTotalCounts: map[string]float64{
				"pod1": 3,
				"pod2": 2,
			},
			expectedDelta: 5,
		},
		{
			name: "mix of count increases, decreases, and not available",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 10},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": 4,
				"pod2": 4,
				"pod3": 2,
			},
			podTotalCounts: map[string]float64{
				"pod1": 6,                 // Count increase
				"pod2": 2,                 // Count decrease
				"pod3": CountNotAvailable, // Count not available
			},
			expectedDelta: 4,
		},
		{
			name:                     "empty lastSawPodCounts",
			initialTimestampedCounts: []TimestampedCount{},
			initialLastSawCounts:     map[string]float64{},
			podTotalCounts: map[string]float64{
				"pod1": 3,
				"pod2": 2,
			},
			expectedDelta: 5,
		},
		{
			name: "empty podTotalCounts",
			initialTimestampedCounts: []TimestampedCount{
				{timestamp: time.Now().Unix() - 10, count: 5},
			},
			initialLastSawCounts: map[string]float64{
				"pod1": 2,
				"pod2": 3,
			},
			podTotalCounts: map[string]float64{},
			expectedDelta:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcQueue := sharedqueue.New[TimestampedCount](1800)
			for _, item := range tc.initialTimestampedCounts {
				tcQueue.Append(item)
			}
			UpdateCountTrackers(tcQueue, tc.initialLastSawCounts, tc.podTotalCounts)
			var delta float64
			if len(tc.initialTimestampedCounts) > 0 {
				delta = float64(tcQueue.Newest().count) - float64(tc.initialTimestampedCounts[len(tc.initialTimestampedCounts)-1].count)
			} else {
				delta = float64(tcQueue.Newest().count)
			}
			assert.Equal(t, tc.expectedDelta, delta)
			// verify that lastSawPodCounts is updated
			for pod, count := range tc.podTotalCounts {
				assert.Equal(t, tc.initialLastSawCounts[pod], count)
			}
		})
	}
}
