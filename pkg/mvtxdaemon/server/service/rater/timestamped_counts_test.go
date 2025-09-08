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

	"github.com/stretchr/testify/assert"
)

func TestNewTimestampedCounts(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodMetricsCount{"pod1", 10.0, TestTime})
	assert.Equal(t, int64(TestTime), tc.timestamp)
	assert.Equal(t, 1, len(tc.podTimeSeries))
	assert.Equal(t, int64(TestTime), tc.podTimeSeries["pod1"].Time)
	assert.Equal(t, 10.0, tc.podTimeSeries["pod1"].Value)
	assert.Contains(t, tc.String(), "timestamp: 1620000000")
	assert.Contains(t, tc.String(), "podTimeSeries:")
}

func TestTimestampedCounts_Update(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodMetricsCount{"pod1", 10.0, TestTime})
	assert.Equal(t, 10.0, tc.podTimeSeries["pod1"].Value)
	assert.Equal(t, int64(TestTime), tc.podTimeSeries["pod1"].Time)
	tc.Update(&PodMetricsCount{"pod1", 20.0, TestTime + 1})
	assert.Equal(t, 20.0, tc.podTimeSeries["pod1"].Value)
	assert.Equal(t, int64(TestTime + 1), tc.podTimeSeries["pod1"].Time)
	tc.Update(&PodMetricsCount{"pod2", 30.0, TestTime + 2})
	assert.Equal(t, 30.0, tc.podTimeSeries["pod2"].Value)
	assert.Equal(t, int64(TestTime + 2), tc.podTimeSeries["pod2"].Time)
	assert.Equal(t, 2, len(tc.podTimeSeries))
	tc.Update(nil)
	assert.Equal(t, 2, len(tc.podTimeSeries))
	assert.Equal(t, 20, int(tc.podTimeSeries["pod1"].Value))
	assert.Equal(t, 30, int(tc.podTimeSeries["pod2"].Value))

	tc.Update(&PodMetricsCount{"pod1", 10.0, TestTime + 3})
	assert.Equal(t, 10, int(tc.podTimeSeries["pod1"].Value))
	assert.Equal(t, int64(TestTime + 3), tc.podTimeSeries["pod1"].Time)
	tc.Update(&PodMetricsCount{"pod2", 20.0, TestTime + 4})
	assert.Equal(t, 20, int(tc.podTimeSeries["pod2"].Value))
	assert.Equal(t, int64(TestTime + 4), tc.podTimeSeries["pod2"].Time)

	tc2 := NewTimestampedCounts(TestTime + 1)
	tc2.Update(&PodMetricsCount{"pod1", 40.0, TestTime + 5})
	assert.Equal(t, 40.0, tc2.podTimeSeries["pod1"].Value)
	assert.Equal(t, int64(TestTime + 5), tc2.podTimeSeries["pod1"].Time)
	tc2.Update(&PodMetricsCount{"pod2", 10.0, TestTime + 6})
	assert.Equal(t, 10.0, tc2.podTimeSeries["pod2"].Value)
	assert.Equal(t, int64(TestTime + 6), tc2.podTimeSeries["pod2"].Time)
}

func TestTimestampedPodCounts_Snapshot(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodMetricsCount{"pod1", 10.0, TestTime})
	tc.Update(&PodMetricsCount{"pod2", 20.0, TestTime + 1})
	assert.Equal(t, map[string]float64{"pod1": 10.0, "pod2": 20.0}, tc.PodCountSnapshot())

	timeSeriesSnapshot := tc.PodTimeSeriesSnapshot()
	assert.Equal(t, 2, len(timeSeriesSnapshot))
	assert.Equal(t, 10.0, timeSeriesSnapshot["pod1"].Value)
	assert.Equal(t, int64(TestTime), timeSeriesSnapshot["pod1"].Time)
	assert.Equal(t, 20.0, timeSeriesSnapshot["pod2"].Value)
	assert.Equal(t, int64(TestTime + 1), timeSeriesSnapshot["pod2"].Time)
}
