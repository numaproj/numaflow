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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/daemon/server/service/rater"
)

func TestNewTimestampedCounts(t *testing.T) {
	tc := NewTimestampedCounts(rater.TestTime)
	tc.Update(&rater.PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	assert.Equal(t, int64(rater.TestTime), tc.timestamp)
	assert.Equal(t, 1, len(tc.podPartitionCount))
	assert.Equal(t, "{timestamp: 1620000000, podPartitionCount: map[pod1:map[partition1:10]]}", tc.String())
}

func TestTimestampedCounts_Update(t *testing.T) {
	tc := NewTimestampedCounts(rater.TestTime)
	tc.Update(&rater.PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	assert.Equal(t, 10.0, tc.podPartitionCount["pod1"]["partition1"])
	tc.Update(&rater.PodReadCount{"pod1", map[string]float64{"partition1": 20.0}})
	assert.Equal(t, 20.0, tc.podPartitionCount["pod1"]["partition1"])
	tc.Update(&rater.PodReadCount{"pod2", map[string]float64{"partition1": 30.0}})
	assert.Equal(t, 30.0, tc.podPartitionCount["pod2"]["partition1"])
	assert.Equal(t, 2, len(tc.podPartitionCount))
	tc.Update(nil)
	assert.Equal(t, 2, len(tc.podPartitionCount))
	assert.Equal(t, 20, int(tc.podPartitionCount["pod1"]["partition1"]))
	assert.Equal(t, 30, int(tc.podPartitionCount["pod2"]["partition1"]))

	tc.Update(&rater.PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	assert.Equal(t, 10, int(tc.podPartitionCount["pod1"]["partition1"]))
	tc.Update(&rater.PodReadCount{"pod2", map[string]float64{"partition1": 20.0}})
	assert.Equal(t, 20, int(tc.podPartitionCount["pod2"]["partition1"]))

	tc2 := NewTimestampedCounts(rater.TestTime + 1)
	tc2.Update(&rater.PodReadCount{"pod1", map[string]float64{"partition1": 40.0}})
	assert.Equal(t, 40.0, tc2.podPartitionCount["pod1"]["partition1"])
	tc2.Update(&rater.PodReadCount{"pod2", map[string]float64{"partition1": 10.0}})
	assert.Equal(t, 10.0, tc2.podPartitionCount["pod2"]["partition1"])
}

func TestTimestampedPodCounts_Snapshot(t *testing.T) {
	tc := NewTimestampedCounts(rater.TestTime)
	tc.Update(&rater.PodReadCount{"pod1", map[string]float64{"partition1": 10.0}})
	tc.Update(&rater.PodReadCount{"pod2", map[string]float64{"partition1": 20.0}})
	assert.Equal(t, map[string]map[string]float64{"pod1": {"partition1": 10.0}, "pod2": {"partition1": 20.0}}, tc.PodPartitionCountSnapshot())
}
