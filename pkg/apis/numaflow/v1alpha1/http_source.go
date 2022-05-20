package v1alpha1

import corev1 "k8s.io/api/core/v1"

type HTTPSource struct {
	// +optional
	Auth *Authorization `json:"auth" protobuf:"bytes,1,opt,name=auth"`
	// Whether to create a ClusterIP Service
	// +optional
	Service bool `json:"service" protobuf:"bytes,2,opt,name=service"`
}

type Authorization struct {
	// A secret selector which contains bearer token
	// To use this, the client needs to add "Authorization: Bearer <token>" in the header
	// +optional
	Token *corev1.SecretKeySelector `json:"token" protobuf:"bytes,1,opt,name=token"`
}
