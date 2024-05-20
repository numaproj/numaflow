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

type GeneratorSource struct {
	// +kubebuilder:default=5
	// +optional
	RPU *int64 `json:"rpu,omitempty" protobuf:"bytes,1,opt,name=rpu"`
	// +kubebuilder:default="1s"
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty" protobuf:"bytes,2,opt,name=duration"`
	// Size of each generated message
	// +kubebuilder:default=8
	// +optional
	MsgSize *int32 `json:"msgSize,omitempty" protobuf:"bytes,3,opt,name=msgSize"`
	// KeyCount is the number of unique keys in the payload
	// +optional
	KeyCount *int32 `json:"keyCount,omitempty" protobuf:"bytes,4,opt,name=keyCount"`
	// Value is an optional uint64 value to be written in to the payload
	// +optional
	Value *uint64 `json:"value,omitempty" protobuf:"bytes,5,opt,name=value"`
	// Jitter is the jitter for the message generation, used to simulate out of order messages
	// for example if the jitter is 10s, then the message's event time will be delayed by a random
	// time between 0 and 10s which will result in the message being out of order by 0 to 10s
	// +kubebuilder:default="0s"
	// +optional
	Jitter *metav1.Duration `json:"jitter,omitempty" protobuf:"bytes,6,opt,name=jitter"`
	// ValueBlob is an optional string which is the base64 encoding of direct payload to send.
	// This is useful for attaching a GeneratorSource to a true pipeline to test load behavior
	// with true messages without requiring additional work to generate messages through
	// the external source
	// if present, the Value and MsgSize fields will be ignored.
	// +optional
	ValueBlob *string `json:"valueBlob,omitempty" protobuf:"bytes,7,opt,name=valueBlob"`
}
