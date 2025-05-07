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

// The RetryStrategy struct defines the configuration for handling operation retries in case of failures.
// It incorporates an Exponential BackOff strategy to control retry timing and specifies the actions to take upon failure.
type RetryStrategy struct {
	// BackOff specifies the parameters for the exponential backoff strategy, controlling how delays between retries should increase.
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
	// Interval sets the initial retry duration, after a failure occurs.
	// +kubebuilder:default="1ms"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty" protobuf:"bytes,1,opt,name=interval"`
	// Steps defines the maximum number of retry attempts
	// +optional
	Steps *uint32 `json:"steps,omitempty" protobuf:"bytes,2,opt,name=steps"`
	// Interval is multiplied by factor each iteration, if factor is not zero
	// and the limits imposed by Steps and Cap have not been reached.
	// +optional
	Factor *float64 `json:"factor,omitempty" protobuf:"bytes,3,opt,name=factor"`
	// A limit on revised values of the interval parameter. If a
	// multiplication by the factor parameter would make the interval
	// exceed the cap then the interval is set to the cap and the
	// steps parameter is set to zero.
	// +optional
	Cap *metav1.Duration `json:"cap,omitempty" protobuf:"bytes,4,opt,name=cap"`
	// The sleep at each iteration is the interval plus an additional
	// amount chosen uniformly at random from the interval between
	// zero and `jitter*interval`.
	// +optional
	Jitter *float64 `json:"jitter,omitempty" protobuf:"bytes,5,opt,name=jitter"`
}

// GetBackoff constructs a wait.Backoff configuration using default values and optionally overrides
// these defaults with custom settings specified in the RetryStrategy.
func (r RetryStrategy) GetBackoff() wait.Backoff {
	// Initialize the Backoff structure with default values.
	// Default backoff strategy is indefinite number of
	// retries with fixed interval of 1ms between retries.
	wt := wait.Backoff{
		Duration: DefaultRetryInterval,
		Steps:    DefaultRetrySteps,
		Factor:   DefaultFactor,
		Cap:      DefaultRetryInterval, // Cap set to the same as Duration for fixed interval
	}

	// If a custom exponential back-off configuration is present, check and substitute the respective parts.
	if r.BackOff != nil {
		// If a custom Interval is specified, override the default Interval.
		if r.BackOff.Interval != nil {
			wt.Duration = r.BackOff.Interval.Duration
		}
		// If custom Steps are specified, override the default Steps.
		if r.BackOff.Steps != nil {
			wt.Steps = int(*r.BackOff.Steps)
		}
		// If a custom Factor is specified, override the default Factor.
		if r.BackOff.Factor != nil {
			wt.Factor = *r.BackOff.Factor
		}
		// If a custom Cap is specified, set the Cap.
		if r.BackOff.Cap != nil {
			wt.Cap = r.BackOff.Cap.Duration
		}
		// If a custom Jitter is specified, set the Jitter.
		if r.BackOff.Jitter != nil {
			wt.Jitter = *r.BackOff.Jitter
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
