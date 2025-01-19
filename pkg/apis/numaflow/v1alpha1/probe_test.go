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

func TestGetProbeInitialDelaySecondsOr(t *testing.T) {
	tests := []struct {
		name         string
		probe        *Probe
		defaultValue int32
		expected     int32
	}{
		{
			name:         "nil probe",
			probe:        nil,
			defaultValue: 10,
			expected:     10,
		},
		{
			name:         "nil InitialDelaySeconds",
			probe:        &Probe{},
			defaultValue: 5,
			expected:     5,
		},
		{
			name:         "non-nil InitialDelaySeconds",
			probe:        &Probe{InitialDelaySeconds: ptr.To[int32](15)},
			defaultValue: 10,
			expected:     15,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetProbeInitialDelaySecondsOr(tt.probe, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetProbeTimeoutSeconds(t *testing.T) {
	tests := []struct {
		name         string
		probe        *Probe
		defaultValue int32
		expected     int32
	}{
		{
			name:         "nil probe",
			probe:        nil,
			defaultValue: 5,
			expected:     5,
		},
		{
			name:         "nil TimeoutSeconds",
			probe:        &Probe{},
			defaultValue: 3,
			expected:     3,
		},
		{
			name:         "non-nil TimeoutSeconds",
			probe:        &Probe{TimeoutSeconds: ptr.To[int32](8)},
			defaultValue: 5,
			expected:     8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetProbeTimeoutSecondsOr(tt.probe, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetProbePeriodSeconds(t *testing.T) {
	tests := []struct {
		name         string
		probe        *Probe
		defaultValue int32
		expected     int32
	}{
		{
			name:         "nil probe",
			probe:        nil,
			defaultValue: 10,
			expected:     10,
		},
		{
			name:         "nil PeriodSeconds",
			probe:        &Probe{},
			defaultValue: 15,
			expected:     15,
		},
		{
			name:         "non-nil PeriodSeconds",
			probe:        &Probe{PeriodSeconds: ptr.To[int32](20)},
			defaultValue: 10,
			expected:     20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetProbePeriodSecondsOr(tt.probe, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetProbeSuccessThreshold(t *testing.T) {
	tests := []struct {
		name         string
		probe        *Probe
		defaultValue int32
		expected     int32
	}{
		{
			name:         "nil probe",
			probe:        nil,
			defaultValue: 1,
			expected:     1,
		},
		{
			name:         "nil SuccessThreshold",
			probe:        &Probe{},
			defaultValue: 2,
			expected:     2,
		},
		{
			name:         "non-nil SuccessThreshold",
			probe:        &Probe{SuccessThreshold: ptr.To[int32](3)},
			defaultValue: 1,
			expected:     3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetProbeSuccessThresholdOr(tt.probe, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetProbeFailureThreshold(t *testing.T) {
	tests := []struct {
		name         string
		probe        *Probe
		defaultValue int32
		expected     int32
	}{
		{
			name:         "nil probe",
			probe:        nil,
			defaultValue: 3,
			expected:     3,
		},
		{
			name:         "nil FailureThreshold",
			probe:        &Probe{},
			defaultValue: 5,
			expected:     5,
		},
		{
			name:         "non-nil FailureThreshold",
			probe:        &Probe{FailureThreshold: ptr.To[int32](7)},
			defaultValue: 3,
			expected:     7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetProbeFailureThresholdOr(tt.probe, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}
