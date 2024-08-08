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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func Test_Scale_Parameters(t *testing.T) {
	s := Scale{}
	assert.Equal(t, int32(0), s.GetMinReplicas())
	assert.Equal(t, int32(DefaultMaxReplicas), s.GetMaxReplicas())
	assert.Equal(t, DefaultCooldownSeconds, s.GetScaleUpCooldownSeconds())
	assert.Equal(t, DefaultCooldownSeconds, s.GetScaleDownCooldownSeconds())
	assert.Equal(t, DefaultLookbackSeconds, s.GetLookbackSeconds())
	assert.Equal(t, DefaultReplicasPerScale, s.GetReplicasPerScale())
	assert.Equal(t, DefaultTargetBufferAvailability, s.GetTargetBufferAvailability())
	assert.Equal(t, DefaultTargetProcessingSeconds, s.GetTargetProcessingSeconds())
	assert.Equal(t, DefaultZeroReplicaSleepSeconds, s.GetZeroReplicaSleepSeconds())
	upcds := uint32(100)
	downcds := uint32(99)
	lbs := uint32(101)
	rps := uint32(3)
	tps := uint32(102)
	tbu := uint32(33)
	zrss := uint32(44)
	s = Scale{
		Min:                      ptr.To[int32](2),
		Max:                      ptr.To[int32](4),
		ScaleUpCooldownSeconds:   &upcds,
		ScaleDownCooldownSeconds: &downcds,
		LookbackSeconds:          &lbs,
		ReplicasPerScale:         &rps,
		TargetProcessingSeconds:  &tps,
		TargetBufferAvailability: &tbu,
		ZeroReplicaSleepSeconds:  &zrss,
	}
	assert.Equal(t, int32(2), s.GetMinReplicas())
	assert.Equal(t, int32(4), s.GetMaxReplicas())
	assert.Equal(t, int(upcds), s.GetScaleUpCooldownSeconds())
	assert.Equal(t, int(downcds), s.GetScaleDownCooldownSeconds())
	assert.Equal(t, int(lbs), s.GetLookbackSeconds())
	assert.Equal(t, int(rps), s.GetReplicasPerScale())
	assert.Equal(t, int(tbu), s.GetTargetBufferAvailability())
	assert.Equal(t, int(tps), s.GetTargetProcessingSeconds())
	assert.Equal(t, int(zrss), s.GetZeroReplicaSleepSeconds())
	s.Max = ptr.To[int32](500)
	assert.Equal(t, int32(500), s.GetMaxReplicas())
}
