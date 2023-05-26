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
	tc := NewTimestampedCounts(1620000000)
	assert.Equal(t, int64(1620000000), tc.timestamp)
	assert.Equal(t, 0, len(tc.podCounts))
}

func TestTimestampedCounts_Update(t *testing.T) {
	tc := NewTimestampedCounts(1620000000)
	tc.Update("pod1", 10.0)
	assert.Equal(t, 10.0, tc.podCounts["pod1"])
	tc.Update("pod1", 20.0)
	assert.Equal(t, 20.0, tc.podCounts["pod1"])
	tc.Update("pod2", 30.0)
	assert.Equal(t, 30.0, tc.podCounts["pod2"])
	assert.Equal(t, 2, len(tc.podCounts))
	tc.Update("pod1", CountNotAvailable)
	assert.Equal(t, 1, len(tc.podCounts))
}

func TestTimestampedCounts_Snapshot(t *testing.T) {
	tc := NewTimestampedCounts(1620000000)
	tc.Update("pod1", 10.0)
	tc.Update("pod2", 20.0)
	tc.Update("pod3", 30.0)
	assert.Equal(t, map[string]float64{"pod1": 10.0, "pod2": 20.0, "pod3": 30.0}, tc.Snapshot())
}
