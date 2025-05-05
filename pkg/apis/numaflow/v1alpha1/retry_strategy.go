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
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type OnFailureRetryStrategy string

// Constants representing the possible actions that can be taken when a failure occurs during an operation.
const (
	OnFailureRetry    OnFailureRetryStrategy = "retry"    // Retry the operation.
	OnFailureFallback OnFailureRetryStrategy = "fallback" // Reroute the operation to a fallback mechanism.
	OnFailureDrop     OnFailureRetryStrategy = "drop"     // Drop the operation and perform no further action.
)

// RetryStrategy struct encapsulates the settings for retrying operations in the event of failures.
// It includes a BackOff strategy to manage the timing of retries and defines the action to take upon failure.
type RetryStrategy struct {
	// BackOff specifies the parameters for the backoff strategy, controlling how delays between retries should increase.
	// +optional
	BackOff *Backoff `json:"backoff,omitempty" protobuf:"bytes,1,opt,name=backoff"`
	// OnFailure specifies the action to take when the specified retry strategy fails.
	// The possible values are:
	// 1. "retry": start another round of retrying the operation,
	// 2. "fallback": re-route the operation to a fallback sink and
	// 3. "drop": drop the operation and perform no further action.
	// The default action is to retry.
	// +optional
	// +kubebuilder:default="retry"
	OnFailure *OnFailureRetryStrategy `json:"onFailure,omitempty" protobuf:"bytes,2,opt,name=onFailure"`
}

// Backoff defines parameters used to systematically configure the retry strategy.
type Backoff struct {
	// Interval sets the initial retry interval, after a failure occurs.
	// +kubebuilder:default="1ms"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty" protobuf:"bytes,1,opt,name=interval"`
	// MaxRetryAttempts defines the maximum number of retry attempts
	// +optional
	MaxRetryAttempts *uint32 `json:"maxRetryAttempts,omitempty" protobuf:"bytes,2,opt,name=maxRetryAttempts"`
	// Multiplier specifies the factor by which the retry interval increases after each attempt.
	// For example, a multiplier of "2.0" doubles the interval after each retry.
	// +optional
	Multiplier *string `json:"multiplier,omitempty" protobuf:"bytes,3,opt,name=multiplier"`
	// MaxInterval specifies the maximum interval between retries, capping the exponential growth.
	// +optional
	MaxInterval *metav1.Duration `json:"maxInterval,omitempty" protobuf:"bytes,4,opt,name=maxInterval"`
}

// GetBackoff constructs a wait.Backoff configuration using default values and optionally overrides
// these defaults with custom settings specified in the RetryStrategy.
func (r RetryStrategy) GetBackoff() wait.Backoff {
	// Initialize the Backoff structure with default values.
	wt := wait.Backoff{
		Duration: DefaultRetryInterval,
		Steps:    DefaultRetrySteps,
		Factor:   1.0,                  // Default multiplier (no exponential backoff by default)
		Cap:      DefaultRetryInterval, // Cap set to the same as Duration for fixed interval
	}

	// If a custom back-off configuration is present, check and substitute the respective parts.
	if r.BackOff != nil {
		// If a custom Interval is specified, override the default Duration.
		if r.BackOff.Interval != nil {
			wt.Duration = r.BackOff.Interval.Duration
		}
		// If custom MaxRetryAttempts are specified, override the default Steps.
		if r.BackOff.MaxRetryAttempts != nil {
			wt.Steps = int(*r.BackOff.MaxRetryAttempts)
		}
		// If a custom Multiplier is specified, override the default Factor.
		if r.BackOff.Multiplier != nil {
			if multiplier, err := strconv.ParseFloat(*r.BackOff.Multiplier, 64); err == nil {
				wt.Factor = multiplier
			} else {
				// Handle the error or set a default value
				wt.Factor = 1.0
			}
		}
		// If a custom MaxInterval is specified, set the Cap.
		if r.BackOff.MaxInterval != nil {
			wt.Cap = r.BackOff.MaxInterval.Duration
		}
	}

	// Returns the fully configured Backoff structure, which is either default or overridden by custom settings.
	return wt
}

// GetOnFailureRetryStrategy retrieves the currently set strategy for handling failures upon retrying.
// This method uses a default strategy which can be overridden by a custom strategy defined in RetryStrategy.
func (r RetryStrategy) GetOnFailureRetryStrategy() OnFailureRetryStrategy {
	// If the OnFailure is not defined initialize with the Default value
	if r.OnFailure == nil {
		return DefaultOnFailureRetryStrategy
	}
	switch *r.OnFailure {
	case OnFailureRetry, OnFailureFallback, OnFailureDrop:
		// If a custom on-failure behavior is specified
		return *r.OnFailure
	default:
		return DefaultOnFailureRetryStrategy
	}
}
