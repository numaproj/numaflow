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

func Test_InitializeConditions(t *testing.T) {
	s := &Status{}
	s.InitializeConditions(ConditionType("c1"), ConditionType("c2"))
	assert.Equal(t, 2, len(s.Conditions))
	types := []string{}
	for _, c := range s.Conditions {
		assert.Equal(t, metav1.ConditionUnknown, c.Status)
		types = append(types, c.Type)
	}
	assert.Contains(t, types, "c1")
	assert.Contains(t, types, "c2")
}

func Test_setCondition(t *testing.T) {
	s := &Status{}
	s.setCondition(metav1.Condition{
		Type:    "test-type",
		Status:  "status",
		Reason:  "reason",
		Message: "message",
	})
	assert.Equal(t, 1, len(s.Conditions))
	assert.Equal(t, "test-type", s.Conditions[0].Type)
	assert.Equal(t, "status", string(s.Conditions[0].Status))
	assert.Equal(t, "reason", s.Conditions[0].Reason)
	assert.Equal(t, "message", s.Conditions[0].Message)
	s.setCondition(metav1.Condition{
		Type:    "test-type",
		Status:  "status1",
		Reason:  "reason1",
		Message: "message1",
	})
	assert.Equal(t, 1, len(s.Conditions))
	assert.Equal(t, "test-type", s.Conditions[0].Type)
	assert.Equal(t, "status1", string(s.Conditions[0].Status))
	assert.Equal(t, "reason1", s.Conditions[0].Reason)
	assert.Equal(t, "message1", s.Conditions[0].Message)
	s.setCondition(metav1.Condition{
		Type:    "test-type2",
		Status:  "status2",
		Reason:  "reason2",
		Message: "message2",
	})
	assert.Equal(t, 2, len(s.Conditions))
}

func Test_markTypeStatus(t *testing.T) {
	s := &Status{}
	s.markTypeStatus("test-type", "status1", "reason1", "message1")
	assert.Equal(t, 1, len(s.Conditions))
	assert.Equal(t, "test-type", s.Conditions[0].Type)
	assert.Equal(t, "status1", string(s.Conditions[0].Status))
	assert.Equal(t, "reason1", s.Conditions[0].Reason)
	assert.Equal(t, "message1", s.Conditions[0].Message)
}

func Test_MarkTrue(t *testing.T) {
	s := &Status{}
	s.MarkTrue("test-type")
	assert.Equal(t, 1, len(s.Conditions))
	assert.Equal(t, "test-type", s.Conditions[0].Type)
	assert.Equal(t, metav1.ConditionTrue, s.Conditions[0].Status)
	assert.Equal(t, "Successful", s.Conditions[0].Reason)
	assert.Equal(t, "Successful", s.Conditions[0].Message)
}

func Test_MarkTrueWithReason(t *testing.T) {
	s := &Status{}
	s.MarkTrueWithReason("test-type", "reason", "message")
	assert.Equal(t, 1, len(s.Conditions))
	assert.Equal(t, "test-type", s.Conditions[0].Type)
	assert.Equal(t, metav1.ConditionTrue, s.Conditions[0].Status)
	assert.Equal(t, "reason", s.Conditions[0].Reason)
	assert.Equal(t, "message", s.Conditions[0].Message)
}

func Test_MarkFalse(t *testing.T) {
	s := &Status{}
	s.MarkFalse("test-type", "reason", "message")
	assert.Equal(t, 1, len(s.Conditions))
	assert.Equal(t, "test-type", s.Conditions[0].Type)
	assert.Equal(t, metav1.ConditionFalse, s.Conditions[0].Status)
	assert.Equal(t, "reason", s.Conditions[0].Reason)
	assert.Equal(t, "message", s.Conditions[0].Message)
}

func Test_MarkUnknown(t *testing.T) {
	s := &Status{}
	s.MarkUnknown("test-type", "reason", "message")
	assert.Equal(t, 1, len(s.Conditions))
	assert.Equal(t, "test-type", s.Conditions[0].Type)
	assert.Equal(t, metav1.ConditionUnknown, s.Conditions[0].Status)
	assert.Equal(t, "reason", s.Conditions[0].Reason)
	assert.Equal(t, "message", s.Conditions[0].Message)
}

func Test_GetCondition(t *testing.T) {
	s := &Status{}
	s.MarkUnknown("test-type1", "reason", "message")
	s.MarkTrue("test-type2")
	m := s.GetCondition(ConditionType("test-type1"))
	assert.NotNil(t, m)
	assert.Equal(t, "reason", m.Reason)
	assert.Equal(t, "message", m.Message)
	m = s.GetCondition(ConditionType("not-existing"))
	assert.Nil(t, m)
	m = s.GetCondition(ConditionType("test-type2"))
	assert.NotNil(t, m)
}

func Test_IsReady(t *testing.T) {
	s := &Status{}
	s.InitializeConditions(ConditionType("type1"), ConditionType("type2"), ConditionType("type3"))
	s.MarkTrue("type1")
	assert.False(t, s.IsReady())
	s.MarkTrue("type2")
	assert.False(t, s.IsReady())
	s.MarkTrue("type3")
	assert.True(t, s.IsReady())
	s.MarkFalse("type2", "reason", "msg")
	assert.False(t, s.IsReady())
}
