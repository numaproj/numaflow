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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_ISBSvcSetPhase(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.SetPhase(ISBSvcPhasePending, "message", 123)
	assert.Equal(t, "message", s.Message)
	assert.Equal(t, ISBSvcPhasePending, s.Phase)
	assert.EqualValues(t, 123, s.ObservedGeneration)
}

func Test_ISBSvcSetType(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.SetType(ISBSvcTypeJetStream)
	assert.Equal(t, ISBSvcTypeJetStream, s.Type)
}

func Test_ISBSvcInit(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.Init()
	assert.Equal(t, 2, len(s.Conditions))
	for _, c := range s.Conditions {
		assert.Equal(t, metav1.ConditionUnknown, c.Status)
		assert.EqualValues(t, -1, c.ObservedGeneration)
	}
	assert.Equal(t, ISBSvcPhasePending, s.Phase)
	assert.EqualValues(t, -1, s.ObservedGeneration)
}

func Test_ISBSvcMarkStatus(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.Init()
	s.MarkNotConfigured("reason", "message", 1)
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionConfigured) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
			assert.Equal(t, "message", c.Message)
			assert.EqualValues(t, 1, c.ObservedGeneration)
		}
	}
	s.MarkConfigured(2)
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionConfigured) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
			assert.EqualValues(t, 2, c.ObservedGeneration)
		}
	}
	s.MarkDeployFailed("reason", "message", 3)
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionDeployed) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
			assert.EqualValues(t, 3, c.ObservedGeneration)
		}
	}
	s.MarkDeployed(4)
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionDeployed) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
			assert.EqualValues(t, 4, c.ObservedGeneration)
		}
	}
	assert.True(t, s.IsReady())
}
