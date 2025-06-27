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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

var testImagePullPolicy = corev1.PullNever

func TestSource_getContainers(t *testing.T) {
	x := Source{
		UDSource: &UDSource{
			Container: &Container{
				Image:        "my-image-s",
				VolumeMounts: []corev1.VolumeMount{{Name: "my-vm"}},
				Command:      []string{"my-cmd-s"},
				Args:         []string{"my-arg-s"},
				Env:          []corev1.EnvVar{{Name: "my-envvar-s"}},
				EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
				}}},
				Resources: corev1.ResourceRequirements{
					Requests: map[corev1.ResourceName]resource.Quantity{
						"cpu": resource.MustParse("2"),
					},
				},
				LivenessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](10),
					TimeoutSeconds:      ptr.To[int32](15),
					PeriodSeconds:       ptr.To[int32](14),
					FailureThreshold:    ptr.To[int32](5),
				},
			},
		},
		UDTransformer: &UDTransformer{
			Container: &Container{
				Image:        "my-image",
				VolumeMounts: []corev1.VolumeMount{{Name: "my-vm"}},
				Command:      []string{"my-cmd"},
				Args:         []string{"my-arg"},
				Env:          []corev1.EnvVar{{Name: "my-envvar"}},
				EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
				}}},
				Resources: corev1.ResourceRequirements{
					Requests: map[corev1.ResourceName]resource.Quantity{
						"cpu": resource.MustParse("2"),
					},
				},
				LivenessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](20),
					TimeoutSeconds:      ptr.To[int32](25),
					PeriodSeconds:       ptr.To[int32](24),
					FailureThreshold:    ptr.To[int32](5),
				},
			},
		},
	}
	sc, c, err := x.getContainers(getContainerReq{
		image: "main-image",
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(sc))
	assert.Equal(t, 1, len(c))
	assert.Equal(t, "main-image", c[0].Image)

	// monitor container
	assert.Equal(t, CtrMonitor, sc[0].Name)
	assert.Equal(t, 1, len(sc[0].VolumeMounts))
	assert.Equal(t, "runtime-vol", sc[0].VolumeMounts[0].Name)

	assert.Equal(t, x.UDSource.Container.Image, sc[2].Image)
	assert.Equal(t, x.UDSource.Container.Command, sc[2].Command)
	assert.Equal(t, x.UDSource.Container.Args, sc[2].Args)
	envsUDSource := map[string]string{}
	for _, e := range sc[2].Env {
		envsUDSource[e.Name] = e.Value
	}
	assert.Equal(t, envsUDSource[EnvUDContainerType], UDContainerSource)
	assert.Equal(t, x.UDSource.Container.EnvFrom, sc[1].EnvFrom)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, sc[1].Resources)
	assert.Equal(t, c[0].ImagePullPolicy, sc[1].ImagePullPolicy)
	assert.NotNil(t, sc[2].LivenessProbe)
	assert.Equal(t, int32(10), sc[2].LivenessProbe.InitialDelaySeconds)
	assert.Equal(t, int32(15), sc[2].LivenessProbe.TimeoutSeconds)
	assert.Equal(t, int32(14), sc[2].LivenessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), sc[2].LivenessProbe.FailureThreshold)
	x.UDSource.Container.ImagePullPolicy = &testImagePullPolicy
	assert.Equal(t, ptr.To[corev1.ContainerRestartPolicy](corev1.ContainerRestartPolicyAlways), sc[0].RestartPolicy)
	assert.Equal(t, ptr.To[corev1.ContainerRestartPolicy](corev1.ContainerRestartPolicyAlways), sc[1].RestartPolicy)
	sc, c, _ = x.getContainers(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, testImagePullPolicy, sc[2].ImagePullPolicy)

	assert.Equal(t, x.UDTransformer.Container.Image, sc[1].Image)
	assert.Contains(t, sc[1].VolumeMounts, sc[1].VolumeMounts[0])
	assert.Equal(t, x.UDTransformer.Container.Command, sc[1].Command)
	assert.Equal(t, x.UDTransformer.Container.Args, sc[1].Args)
	envs := map[string]string{}
	for _, e := range sc[1].Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, envs[EnvUDContainerType], UDContainerTransformer)
	assert.Equal(t, x.UDTransformer.Container.EnvFrom, sc[1].EnvFrom)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, sc[1].Resources)
	assert.Equal(t, c[0].ImagePullPolicy, sc[1].ImagePullPolicy)
	assert.NotNil(t, sc[1].LivenessProbe)
	assert.Equal(t, int32(20), sc[1].LivenessProbe.InitialDelaySeconds)
	assert.Equal(t, int32(25), sc[1].LivenessProbe.TimeoutSeconds)
	assert.Equal(t, int32(24), sc[1].LivenessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), sc[1].LivenessProbe.FailureThreshold)
	x.UDTransformer.Container.ImagePullPolicy = &testImagePullPolicy
	sc, c, _ = x.getContainers(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, corev1.PullAlways, c[0].ImagePullPolicy)
	assert.Equal(t, testImagePullPolicy, sc[1].ImagePullPolicy)
}

func Test_getTransformerContainer(t *testing.T) {
	t.Run("with customized image", func(t *testing.T) {
		x := Source{
			HTTP: &HTTPSource{},
			UDTransformer: &UDTransformer{
				Container: &Container{
					Image:           "my-image",
					Args:            []string{"my-arg"},
					SecurityContext: &corev1.SecurityContext{},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
					}}},
					ImagePullPolicy: &testImagePullPolicy,
				},
			},
		}
		c := x.getUDTransformerContainer(getContainerReq{
			image:           "main-image",
			imagePullPolicy: corev1.PullAlways,
		})
		assert.Equal(t, "my-image", c.Image)
		assert.Equal(t, corev1.PullNever, c.ImagePullPolicy)
		assert.Contains(t, c.Args, "my-arg")
		assert.NotNil(t, c.SecurityContext)
		assert.Equal(t, x.UDTransformer.Container.EnvFrom, c.EnvFrom)
		assert.Equal(t, testImagePullPolicy, c.ImagePullPolicy)
		assert.True(t, c.LivenessProbe != nil)
		envs := map[string]string{}
		for _, e := range c.Env {
			envs[e.Name] = e.Value
		}
		assert.Equal(t, envs[EnvUDContainerType], UDContainerTransformer)
	})
}
