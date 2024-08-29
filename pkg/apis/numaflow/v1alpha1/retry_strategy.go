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
)

type OnFailRetryStrategy string

// Constants representing the possible actions that can be taken when a failure occurs during an operation.
const (
	OnFailRetry    OnFailRetryStrategy = "retry"    // Retry the operation.
	OnFailFallback OnFailRetryStrategy = "fallback" // Reroute the operation to a fallback mechanism.
	OnFailDrop     OnFailRetryStrategy = "drop"     // Drop the operation and perform no further action.
)

// RetryStrategy struct encapsulates the settings for retrying operations in the event of failures.
// It includes a BackOff strategy to manage the timing of retries and defines the action to take upon failure.
type RetryStrategy struct {
	// BackOff specifies the parameters for the backoff strategy, controlling how delays between retries should increase.
	// +optional
	BackOff *Backoff `json:"backoff,omitempty" protobuf:"bytes,1,opt,name=backoff"`
	// OnFailure specifies the action to take when a retry fails. The default action is to retry.
	// +optional
	// +kubebuilder:default="retry"
	OnFailure *OnFailRetryStrategy `json:"onFailure,omitempty" protobuf:"bytes,2,opt,name=onFailure"`
}

// Backoff defines parameters used to systematically configure the retry strategy.
type Backoff struct {
	// Interval sets the delay to wait before retry, after a failure occurs.
	// +kubebuilder:default="1ms"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty" protobuf:"bytes,1,opt,name=interval"`
	// Steps defines the number of times to retry after a failure occurs.
	// +optional
	// +kubebuilder:default=0
	Steps *uint32 `json:"steps,omitempty" protobuf:"bytes,2,opt,name=steps"`
	// TODO(Retry): Enable after we add support for exponential backoff
	//// +optional
	//Cap *metav1.Duration `json:"cap,omitempty" protobuf:"bytes,3,opt,name=cap"`
	//// +optional
	//Factor *floatstr `json:"factor,omitempty" protobuf:"bytes,2,opt,name=factor"`
	//// +optional
	//Jitter *floatstr `json:"jitter,omitempty" protobuf:"bytes,3,opt,name=jitter"`
}
