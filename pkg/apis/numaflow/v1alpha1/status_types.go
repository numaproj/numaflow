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
	"reflect"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionType is a valid value of Condition.Type
type ConditionType string

const (
	// ConditionReady indicates the resource is ready.
	ConditionReady ConditionType = "Ready"
)

// Status is a common structure which can be used for Status field.
type Status struct {
	// Conditions are the latest available observations of a resource's current state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// InitializeConditions initializes the conditions to Unknown
func (s *Status) InitializeConditions(conditionTypes ...ConditionType) {
	for _, t := range conditionTypes {
		c := metav1.Condition{
			Type:   string(t),
			Status: metav1.ConditionUnknown,
			Reason: "Unknown",
		}
		s.setCondition(c)
	}
}

// setCondition sets a condition
func (s *Status) setCondition(condition metav1.Condition) {
	var conditions []metav1.Condition
	for _, c := range s.Conditions {
		if c.Type != condition.Type {
			conditions = append(conditions, c)
		} else {
			condition.LastTransitionTime = c.LastTransitionTime
			if reflect.DeepEqual(&condition, &c) {
				return
			}
		}
	}
	condition.LastTransitionTime = metav1.NewTime(time.Now())
	conditions = append(conditions, condition)
	// Sort for easy read
	sort.Slice(conditions, func(i, j int) bool { return conditions[i].Type < conditions[j].Type })
	s.Conditions = conditions
}

func (s *Status) markTypeStatus(t ConditionType, status metav1.ConditionStatus, reason, message string) {
	s.setCondition(metav1.Condition{
		Type:    string(t),
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// MarkTrue sets the status of t to true
func (s *Status) MarkTrue(t ConditionType) {
	s.markTypeStatus(t, metav1.ConditionTrue, "Successful", "Successful")
}

// MarkTrueWithReason sets the status of t to true with reason
func (s *Status) MarkTrueWithReason(t ConditionType, reason, message string) {
	s.markTypeStatus(t, metav1.ConditionTrue, reason, message)
}

// MarkFalse sets the status of t to fasle
func (s *Status) MarkFalse(t ConditionType, reason, message string) {
	s.markTypeStatus(t, metav1.ConditionFalse, reason, message)
}

// MarkUnknown sets the status of t to unknown
func (s *Status) MarkUnknown(t ConditionType, reason, message string) {
	s.markTypeStatus(t, metav1.ConditionUnknown, reason, message)
}

// GetCondition returns the condition of a condition type
func (s *Status) GetCondition(t ConditionType) *metav1.Condition {
	for _, c := range s.Conditions {
		if c.Type == string(t) {
			return &c
		}
	}
	return nil
}

// IsReady returns true when all the conditions are true
func (s *Status) IsReady() bool {
	if len(s.Conditions) == 0 {
		return false
	}
	for _, c := range s.Conditions {
		if c.Status != metav1.ConditionTrue {
			return false
		}
	}
	return true
}
