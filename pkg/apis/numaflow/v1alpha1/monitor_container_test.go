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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
)

func TestBuildMonitorContainer(t *testing.T) {
	req := getContainerReq{
		env: []corev1.EnvVar{
			{Name: "TEST_ENV", Value: "test-value"},
		},
		image:           "test-image",
		imagePullPolicy: corev1.PullIfNotPresent,
		resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
		},
		volumeMounts: []corev1.VolumeMount{
			{Name: "test-volume", MountPath: "/test-path"},
		},
	}

	container := buildMonitorContainer(req)

	// Verify container properties
	assert.Equal(t, CtrMonitor, container.Name, "Container name mismatch")
	assert.Equal(t, "test-image", container.Image, "Container image mismatch")
	assert.Equal(t, corev1.PullIfNotPresent, container.ImagePullPolicy, "Image pull policy mismatch")
	assert.Equal(t, "monitor", container.Args[0], "Container args mismatch")
	assert.Equal(t, NumaflowRustBinary, container.Command[0], "Container command mismatch")

	// Verify resource requirements
	assert.Equal(t, container.Resources.Limits, corev1.ResourceList{}, "Resource limits mismatch")
	assert.Equal(t, container.Resources.Requests, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("10m"),
		corev1.ResourceMemory: resource.MustParse("20Mi"),
	}, "Resource requests mismatch")

	// Verify volume mounts --> should overwrite req's volume mounts
	assert.Equal(t, 1, len(container.VolumeMounts), "Volume mount count mismatch")
	assert.Equal(t, "runtime-vol", container.VolumeMounts[0].Name, "Volume mount name mismatch")
	assert.Equal(t, "/var/numaflow/runtime", container.VolumeMounts[0].MountPath, "Volume mount path mismatch")

	// Verify restart policy (sidecar-specific behavior)
	if isSidecarSupported() {
		assert.NotNil(t, container.RestartPolicy, "Restart policy should not be nil for sidecar")
		assert.Equal(t, corev1.ContainerRestartPolicyAlways, *container.RestartPolicy, "Restart policy mismatch for sidecar")
	}
}
