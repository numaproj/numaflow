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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum="";Running;Failed;Pausing;Paused;Deleting
type MonoVertexPhase string

const (
	MonoVertexPhaseUnknown MonoVertexPhase = ""
	MonoVertexPhaseRunning MonoVertexPhase = "Running"
	MonoVertexPhaseFailed  MonoVertexPhase = "Failed"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=mv
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type MonoVertex struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec MonoVertexSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// +optional
	Status MonoVertexStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

type MonoVertexSpec struct {
	Source *Source `json:"source,omitempty" protobuf:"bytes,1,opt,name=source"`
	Sink   *Sink   `json:"sink,omitempty" protobuf:"bytes,2,opt,name=sink"`
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,3,opt,name=abstractPodTemplate"`
	// Container template for the main numa container.
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,4,opt,name=containerTemplate"`
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=name
	Volumes []corev1.Volume `json:"volumes,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,5,rep,name=volumes"`
	// Settings for autoscaling
	// +optional
	Scale Scale `json:"scale,omitempty" protobuf:"bytes,6,opt,name=scale"`
	// List of customized init containers belonging to the pod.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
	// +optional
	InitContainers []corev1.Container `json:"initContainers,omitempty" protobuf:"bytes,7,rep,name=initContainers"`
	// List of customized sidecar containers belonging to the pod.
	// +optional
	Sidecars []corev1.Container `json:"sidecars,omitempty" protobuf:"bytes,8,rep,name=sidecars"`
	// Template for the daemon service deployment.
	// +optional
	DaemonTemplate *DaemonTemplate `json:"daemonTemplate,omitempty" protobuf:"bytes,9,opt,name=daemonTemplate"`
}

type MonoVertexStatus struct {
	Status             `json:",inline" protobuf:"bytes,1,opt,name=status"`
	Phase              MonoVertexPhase `json:"phase,omitempty" protobuf:"bytes,2,opt,name=phase,casttype=MonoVertexPhase"`
	Message            string          `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	LastUpdated        metav1.Time     `json:"lastUpdated,omitempty" protobuf:"bytes,4,opt,name=lastUpdated"`
	ObservedGeneration int64           `json:"observedGeneration,omitempty" protobuf:"varint,11,opt,name=observedGeneration"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MonoVertexList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []MonoVertex `json:"items" protobuf:"bytes,2,rep,name=items"`
}
