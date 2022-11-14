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
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (ps PersistenceStrategy) GetPVCSpec(name string) corev1.PersistentVolumeClaim {
	volMode := corev1.PersistentVolumeFilesystem
	// Default volume size
	volSize := apiresource.MustParse("20Gi")
	if ps.VolumeSize != nil {
		volSize = *ps.VolumeSize
	}
	// Default to ReadWriteOnce
	accessMode := corev1.ReadWriteOnce
	if ps.AccessMode != nil {
		accessMode = *ps.AccessMode
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				accessMode,
			},
			VolumeMode:       &volMode,
			StorageClassName: ps.StorageClassName,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: volSize,
				},
			},
		},
	}
}
