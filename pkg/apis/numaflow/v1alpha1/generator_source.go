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
