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

import corev1 "k8s.io/api/core/v1"

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
	return containerBuilder{}.init(req).
		volumeMounts(getRuntimeVolumeMount()...).
		name(CtrMonitor).
		asSidecar().
		command(NumaflowRustBinary).
		args("--monitor").
		build()
}
