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
	resource "k8s.io/apimachinery/pkg/api/resource"
)

// volume mount to the runtime path
func getRuntimeVolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      RuntimeDirVolume,
			MountPath: RuntimeDirMountPath,
		},
	}
}
func buildMonitorContainer(req getContainerReq) corev1.Container {
	return containerBuilder{}.
		image(req.image).
		imagePullPolicy(req.imagePullPolicy).
		resources(corev1.ResourceRequirements{
			Limits: corev1.ResourceList{},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("20Mi"),
			},
		}).
		volumeMounts(getRuntimeVolumeMount()...).
		name(CtrMonitor).
		asSidecar().
		command(NumaflowRustBinary).
		args("monitor").
		build()
}
