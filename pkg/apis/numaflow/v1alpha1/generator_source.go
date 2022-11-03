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
}
