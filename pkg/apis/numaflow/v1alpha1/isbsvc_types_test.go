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
	s.SetPhase(ISBSvcPhasePending, "message")
	assert.Equal(t, "message", s.Message)
	assert.Equal(t, ISBSvcPhasePending, s.Phase)
}

func Test_ISBSvcSetType(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.SetType(ISBSvcTypeJetStream)
	assert.Equal(t, ISBSvcTypeJetStream, s.Type)
}

func Test_ISBSvcInitConditions(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.InitConditions()
	assert.Equal(t, 2, len(s.Conditions))
	for _, c := range s.Conditions {
		assert.Equal(t, metav1.ConditionUnknown, c.Status)
	}
	assert.Equal(t, ISBSvcPhasePending, s.Phase)
}

func Test_ISBSvcMarkStatus(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.InitConditions()
	s.MarkNotConfigured("reason", "message")
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionConfigured) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	s.MarkConfigured()
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionConfigured) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
	s.MarkDeployFailed("reason", "message")
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionDeployed) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	s.MarkDeployed()
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionDeployed) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
	assert.True(t, s.IsReady())
}
