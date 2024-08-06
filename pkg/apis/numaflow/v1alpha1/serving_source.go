package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServingSource is the HTTP endpoint for Numaflow.
type ServingSource struct {
	// +optional
	Auth *Authorization `json:"auth" protobuf:"bytes,1,opt,name=auth"`
	// Whether to create a ClusterIP Service
	// +optional
	Service bool `json:"service" protobuf:"bytes,2,opt,name=service"`
	// The header key from which the message id will be extracted
	MsgIDHeaderKey *string `json:"msgIDHeaderKey" protobuf:"bytes,3,opt,name=msgIDHeaderKey"`
	// Persistent store for the callbacks for serving and tracking
	Store *ServingStore `json:"store" protobuf:"bytes,4,opt,name=store"`
}

// ServingStore to track and store data and metadata for tracking and serving.
type ServingStore struct {
	// URL of the persistent store to write the callbacks
	URL *string `json:"url" protobuf:"bytes,1,opt,name=url"`
	// TTL for the data in the store and tracker
	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty" protobuf:"bytes,2,opt,name=ttl"`
	// TODO auth
}

// GetTTL returns the TTL for the data in the store. If the TTL is not set, it returns 24 hours.
func (ss *ServingStore) GetTTL() *metav1.Duration {
	if ss.TTL == nil {
		return &metav1.Duration{Duration: DefaultServingTTL}
	}
	return ss.TTL
}
