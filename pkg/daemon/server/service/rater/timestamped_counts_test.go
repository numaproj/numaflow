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

	"github.com/stretchr/testify/assert"
)

func TestNewTimestampedCounts(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	assert.Equal(t, int64(TestTime), tc.timestamp)
	assert.Equal(t, 0, len(tc.podPartitionDelta))
	assert.Equal(t, 0, len(tc.podPartitionCount))
	assert.Equal(t, false, tc.isWindowClosed)
}

func TestTimestampedCounts_Update(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	assert.Equal(t, 10.0, tc.podPartitionCount["pod1"]["partition1"])
	tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})
	assert.Equal(t, 20.0, tc.podPartitionCount["pod1"]["partition1"])
	tc.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 30.0}})
	assert.Equal(t, 30.0, tc.podPartitionCount["pod2"]["partition1"])
	assert.Equal(t, 2, len(tc.podPartitionCount))
	tc.Update(nil)
	assert.Equal(t, 2, len(tc.podPartitionCount))
	assert.Equal(t, 20, int(tc.podPartitionCount["pod1"]["partition1"]))
	assert.Equal(t, 30, int(tc.podPartitionCount["pod2"]["partition1"]))
	assert.Equal(t, false, tc.isWindowClosed)

	tc.CloseWindow(nil)
	assert.Equal(t, true, tc.isWindowClosed)
	// verify that updating partition counts doesn't take effect if the window is already closed
	tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	assert.Equal(t, 20, int(tc.podPartitionCount["pod1"]["partition1"]))
	tc.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 20.0}})
	assert.Equal(t, 30, int(tc.podPartitionCount["pod2"]["partition1"]))

	tc2 := NewTimestampedCounts(TestTime + 1)
	tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 40.0}})
	assert.Equal(t, 40.0, tc2.podPartitionCount["pod1"]["partition1"])
	tc2.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 10.0}})
	assert.Equal(t, 10.0, tc2.podPartitionCount["pod2"]["partition1"])
	tc2.CloseWindow(tc)
	assert.Equal(t, true, tc2.isWindowClosed)
}

func TestTimestampedPodCounts_Snapshot(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	tc.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 20.0}})
	assert.Equal(t, map[string]map[string]float64{"pod1": {"partition1": 10.0}, "pod2": {"partition1": 20.0}}, tc.PodReadCountSnapshot())
}

func TestTimestampedPodDeltas_Snapshot(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	tc.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 20.0}})
	tc.CloseWindow(nil)

	tc1 := NewTimestampedCounts(TestTime + 1)
	tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})
	tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 30.0}})
	tc1.CloseWindow(tc)

	assert.Equal(t, map[string]map[string]float64{"pod1": {"partition1": 10.0}, "pod2": {"partition1": 10.0}}, tc1.PodDeltaCountSnapshot())
}

func TestTimestamped_CloseWindow(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	tc.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 20.0}})
	tc.CloseWindow(nil)

	// verify that pod1 restart should give the new count instead of the difference
	tc1 := NewTimestampedCounts(TestTime + 1)
	tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 5.0}})
	tc1.Update(&PodReadCount{"pod2", map[string]float64{"partition1": 30.0}})
	tc1.CloseWindow(tc)

	assert.Equal(t, map[string]map[string]float64{"pod1": {"partition1": 5.0}, "pod2": {"partition1": 10.0}}, tc1.PodDeltaCountSnapshot())
}
