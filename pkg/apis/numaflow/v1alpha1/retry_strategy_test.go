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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

func TestGetBackoff(t *testing.T) {
	steps := uint32(10)
	tests := []struct {
		name            string
		strategy        RetryStrategy
		expectedBackoff wait.Backoff
		steps           uint32
	}{
		{
			name:     "default backoff",
			strategy: RetryStrategy{},
			expectedBackoff: wait.Backoff{
				Duration: DefaultRetryInterval,
				Steps:    DefaultRetrySteps,
			},
		},
		{
			name: "custom backoff",
			strategy: RetryStrategy{
				BackOff: &Backoff{
					Interval: &metav1.Duration{Duration: 10 * time.Second},
					Steps:    &steps,
				},
			},
			expectedBackoff: wait.Backoff{
				Duration: 10 * time.Second,
				Steps:    10,
			},
		},
		{
			name: "custom backoff - 2",
			strategy: RetryStrategy{
				BackOff: &Backoff{
					Interval: &metav1.Duration{Duration: 10 * time.Second},
				},
			},
			expectedBackoff: wait.Backoff{
				Duration: 10 * time.Second,
				Steps:    DefaultRetrySteps,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.strategy.GetBackoff()
			if got.Duration != tt.expectedBackoff.Duration || got.Steps != tt.expectedBackoff.Steps {
				t.Errorf("GetBackoff() = %v, want %v", got, tt.expectedBackoff)
			}
		})
	}
}

func TestGetOnFailureRetryStrategy(t *testing.T) {
	tests := []struct {
		name              string
		strategy          RetryStrategy
		expectedOnFailure OnFailureRetryStrategy
	}{
		{
			name:              "default strategy",
			strategy:          RetryStrategy{},
			expectedOnFailure: DefaultOnFailureRetryStrategy,
		},
		{
			name: "custom strategy",
			strategy: RetryStrategy{
				OnFailure: func() *OnFailureRetryStrategy { s := OnFailureDrop; return &s }(),
			},
			expectedOnFailure: OnFailureDrop,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.strategy.GetOnFailureRetryStrategy()
			if got != tt.expectedOnFailure {
				t.Errorf("GetOnFailureRetryStrategy() = %v, want %v", got, tt.expectedOnFailure)
			}
		})
	}
}
