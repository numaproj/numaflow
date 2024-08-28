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
	"k8s.io/apimachinery/pkg/util/intstr"
)

type OnFailRetryStrategy string

// Constants representing the possible actions that can be taken when a failure occurs during an operation.
const (
	// OnFailRetry - Retry the operation.
	OnFailRetry OnFailRetryStrategy = "retry"
	// OnFailFallback - Reroute the message to a fallback sink.
	OnFailFallback OnFailRetryStrategy = "fallback"
	// OnFailDrop -  Drop the message and perform no further action.
	OnFailDrop OnFailRetryStrategy = "drop"
)

// RetryStrategy defines the criteria and method for retrying a failed write operation in the Sink.
// This type is used to customize how retries are handled, ensuring messages that fail to be delivered
// can be resent based on the configured strategy. It includes settings for fixed interval retry
// strategy and specific actions to take on failures.
type RetryStrategy struct {
	// Timeout sets the time to wait before the next retry, after a failure occurs.
	// +kubebuilder:default="1ms"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty" protobuf:"bytes,1,opt,name=timeout"`
	// +optional
	// +kubebuilder:default=0
	RetryCount *intstr.IntOrString `json:"retryCount,omitempty" protobuf:"bytes,2,opt,name=retryCount"`
	// OnFailure specifies the action to take when a retry fails. The default action is to retry.
	// +optional
	// +kubebuilder:default="retry"
	OnFailure *OnFailRetryStrategy `json:"onFailure,omitempty" protobuf:"bytes,3,opt,name=onFailure"`
}
