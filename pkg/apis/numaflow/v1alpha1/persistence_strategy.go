package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
)

// PersistenceStrategy defines the strategy of persistence
type PersistenceStrategy struct {
	// Name of the StorageClass required by the claim.
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty" protobuf:"bytes,1,opt,name=storageClassName"`
	// Available access modes such as ReadWriteOnce, ReadWriteMany
	// https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes
	// +optional
	AccessMode *corev1.PersistentVolumeAccessMode `json:"accessMode,omitempty" protobuf:"bytes,2,opt,name=accessMode,casttype=k8s.io/api/core/v1.PersistentVolumeAccessMode"`
	// Volume size, e.g. 50Gi
	VolumeSize *apiresource.Quantity `json:"volumeSize,omitempty" protobuf:"bytes,3,opt,name=volumeSize"`
}
