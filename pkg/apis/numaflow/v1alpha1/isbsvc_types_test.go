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
	assert.Equal(t, 3, len(s.Conditions))
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
	s.MarkChildrenResourceUnHealthy("reason", "message")
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionChildrenResourcesHealthy) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	s.MarkChildrenResourceHealthy("RolloutFinished", "All service healthy")
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionChildrenResourcesHealthy) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
			assert.Equal(t, "RolloutFinished", c.Reason)
			assert.Equal(t, "All service healthy", c.Message)
		}
	}
	assert.True(t, s.IsReady())
}

func Test_ISBSvcIsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		status   InterStepBufferServiceStatus
		expected bool
	}{
		{
			name: "Running and Ready",
			status: InterStepBufferServiceStatus{
				Phase: ISBSvcPhaseRunning,
				Status: Status{
					Conditions: []metav1.Condition{
						{Type: string(ISBSvcConditionConfigured), Status: metav1.ConditionTrue},
						{Type: string(ISBSvcConditionDeployed), Status: metav1.ConditionTrue},
					},
				},
			},
			expected: true,
		},
		{
			name: "Running but not Ready",
			status: InterStepBufferServiceStatus{
				Phase: ISBSvcPhaseRunning,
				Status: Status{
					Conditions: []metav1.Condition{
						{Type: string(ISBSvcConditionConfigured), Status: metav1.ConditionTrue},
						{Type: string(ISBSvcConditionDeployed), Status: metav1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			name: "Not Running but Ready",
			status: InterStepBufferServiceStatus{
				Phase: ISBSvcPhasePending,
				Status: Status{
					Conditions: []metav1.Condition{
						{Type: string(ISBSvcConditionConfigured), Status: metav1.ConditionTrue},
						{Type: string(ISBSvcConditionDeployed), Status: metav1.ConditionTrue},
					},
				},
			},
			expected: false,
		},
		{
			name: "Not Running and not Ready",
			status: InterStepBufferServiceStatus{
				Phase: ISBSvcPhasePending,
				Status: Status{
					Conditions: []metav1.Condition{
						{Type: string(ISBSvcConditionConfigured), Status: metav1.ConditionFalse},
						{Type: string(ISBSvcConditionDeployed), Status: metav1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsHealthy()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInterStepBufferService_GetType(t *testing.T) {
	tests := []struct {
		name string
		isbs InterStepBufferService
		want ISBSvcType
	}{
		{
			name: "JetStream type",
			isbs: InterStepBufferService{
				Spec: InterStepBufferServiceSpec{
					JetStream: &JetStreamBufferService{},
				},
			},
			want: ISBSvcTypeJetStream,
		},
		{
			name: "Unknown type",
			isbs: InterStepBufferService{
				Spec: InterStepBufferServiceSpec{},
			},
			want: ISBSvcTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.isbs.GetType()
			assert.Equal(t, tt.want, got)
		})
	}
}
