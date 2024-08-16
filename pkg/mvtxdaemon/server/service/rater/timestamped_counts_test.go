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
	tc.Update(&PodReadCount{"pod1", 10.0})
	assert.Equal(t, int64(TestTime), tc.timestamp)
	assert.Equal(t, 1, len(tc.podReadCounts))
	assert.Equal(t, "{timestamp: 1620000000, podReadCounts: map[pod1:10]}", tc.String())
}

func TestTimestampedCounts_Update(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodReadCount{"pod1", 10.0})
	assert.Equal(t, 10.0, tc.podReadCounts["pod1"])
	tc.Update(&PodReadCount{"pod1", 20.0})
	assert.Equal(t, 20.0, tc.podReadCounts["pod1"])
	tc.Update(&PodReadCount{"pod2", 30.0})
	assert.Equal(t, 30.0, tc.podReadCounts["pod2"])
	assert.Equal(t, 2, len(tc.podReadCounts))
	tc.Update(nil)
	assert.Equal(t, 2, len(tc.podReadCounts))
	assert.Equal(t, 20, int(tc.podReadCounts["pod1"]))
	assert.Equal(t, 30, int(tc.podReadCounts["pod2"]))

	tc.Update(&PodReadCount{"pod1", 10.0})
	assert.Equal(t, 10, int(tc.podReadCounts["pod1"]))
	tc.Update(&PodReadCount{"pod2", 20.0})
	assert.Equal(t, 20, int(tc.podReadCounts["pod2"]))

	tc2 := NewTimestampedCounts(TestTime + 1)
	tc2.Update(&PodReadCount{"pod1", 40.0})
	assert.Equal(t, 40.0, tc2.podReadCounts["pod1"])
	tc2.Update(&PodReadCount{"pod2", 10.0})
	assert.Equal(t, 10.0, tc2.podReadCounts["pod2"])
}

func TestTimestampedPodCounts_Snapshot(t *testing.T) {
	tc := NewTimestampedCounts(TestTime)
	tc.Update(&PodReadCount{"pod1", 10.0})
	tc.Update(&PodReadCount{"pod2", 20.0})
	assert.Equal(t, map[string]float64{"pod1": 10.0, "pod2": 20.0}, tc.PodCountSnapshot())
}
