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
	"strings"
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
	assert.Equal(t, 2, len(sc))
	assert.Equal(t, 1, len(c))
	assert.Equal(t, "main-image", c[0].Image)

	assert.Equal(t, x.UDSource.Container.Image, sc[1].Image)
	assert.Contains(t, sc[1].VolumeMounts, sc[1].VolumeMounts[0])
	assert.Equal(t, x.UDSource.Container.Command, sc[1].Command)
	assert.Equal(t, x.UDSource.Container.Args, sc[1].Args)
	envsUDSource := map[string]string{}
	for _, e := range sc[1].Env {
		envsUDSource[e.Name] = e.Value
	}
	assert.Equal(t, envsUDSource[EnvUDContainerType], UDContainerSource)
	assert.Equal(t, x.UDSource.Container.EnvFrom, sc[1].EnvFrom)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, sc[1].Resources)
	assert.Equal(t, c[0].ImagePullPolicy, sc[1].ImagePullPolicy)
	assert.NotNil(t, sc[0].LivenessProbe)
	assert.Equal(t, int32(10), sc[1].LivenessProbe.InitialDelaySeconds)
	assert.Equal(t, int32(15), sc[1].LivenessProbe.TimeoutSeconds)
	assert.Equal(t, int32(14), sc[1].LivenessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), sc[1].LivenessProbe.FailureThreshold)
	x.UDSource.Container.ImagePullPolicy = &testImagePullPolicy
	assert.Equal(t, ptr.To[corev1.ContainerRestartPolicy](corev1.ContainerRestartPolicyAlways), sc[0].RestartPolicy)
	assert.Equal(t, ptr.To[corev1.ContainerRestartPolicy](corev1.ContainerRestartPolicyAlways), sc[1].RestartPolicy)
	sc, c, _ = x.getContainers(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, testImagePullPolicy, sc[1].ImagePullPolicy)

	assert.Equal(t, x.UDTransformer.Container.Image, sc[0].Image)
	assert.Contains(t, sc[0].VolumeMounts, sc[0].VolumeMounts[0])
	assert.Equal(t, x.UDTransformer.Container.Command, sc[0].Command)
	assert.Equal(t, x.UDTransformer.Container.Args, sc[0].Args)
	envs := map[string]string{}
	for _, e := range sc[0].Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, envs[EnvUDContainerType], UDContainerTransformer)
	assert.Equal(t, x.UDTransformer.Container.EnvFrom, sc[0].EnvFrom)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, sc[0].Resources)
	assert.Equal(t, c[0].ImagePullPolicy, sc[0].ImagePullPolicy)
	assert.NotNil(t, sc[0].LivenessProbe)
	assert.Equal(t, int32(20), sc[0].LivenessProbe.InitialDelaySeconds)
	assert.Equal(t, int32(25), sc[0].LivenessProbe.TimeoutSeconds)
	assert.Equal(t, int32(24), sc[0].LivenessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), sc[0].LivenessProbe.FailureThreshold)
	x.UDTransformer.Container.ImagePullPolicy = &testImagePullPolicy
	sc, c, _ = x.getContainers(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, corev1.PullAlways, c[0].ImagePullPolicy)
	assert.Equal(t, testImagePullPolicy, sc[0].ImagePullPolicy)
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

	t.Run("with built-in transformers", func(t *testing.T) {
		x := Source{
			HTTP: &HTTPSource{},
			UDTransformer: &UDTransformer{
				Container: &Container{
					SecurityContext: &corev1.SecurityContext{},
				},
				Builtin: &Transformer{
					Name: "filter",
					KWArgs: map[string]string{
						"expression": "json(payload).a > 1",
					},
				},
			}}
		c := x.getUDTransformerContainer(getContainerReq{
			image:           "main-image",
			imagePullPolicy: corev1.PullNever,
			env: []corev1.EnvVar{
				{Name: "a", Value: "b"},
			},
		})
		assert.NotNil(t, c.SecurityContext)
		assert.Equal(t, "main-image", c.Image)
		assert.Equal(t, corev1.PullNever, c.ImagePullPolicy)
		assert.Contains(t, c.Args, "--name=filter")
		// log.Print(c.Args)
		envNames := []string{}
		for _, e := range c.Env {
			envNames = append(envNames, e.Name)
		}
		assert.NotContains(t, envNames, "a")
		assert.Contains(t, envNames, EnvUDContainerType)
		assert.True(t, c.LivenessProbe != nil)
	})

	t.Run("with built-in transformers, with multiple KWArgs", func(t *testing.T) {
		// This test verify when we have multiple KWArgs in map, after converting them to a single `--kwargs=a=x,b=y...` string, we maintain a consistent order.
		x := Source{
			HTTP: &HTTPSource{},
			UDTransformer: &UDTransformer{
				Builtin: &Transformer{
					Name: "filter",
					KWArgs: map[string]string{
						"expression": "json(payload).time",
						"format":     "2021-01-01",
						"extra-arg1": "1",
						"extra-arg2": "2",
						"extra-arg3": "3",
						"extra-arg4": "4",
					},
				},
			}}
		c1 := x.getUDTransformerContainer(getContainerReq{
			image: "main-image",
		})
		c2 := x.getUDTransformerContainer(getContainerReq{
			image: "main-image",
		})
		assert.Equal(t, getKWArgs(c1), getKWArgs(c2))
	})
}

func getKWArgs(container corev1.Container) string {
	for _, a := range container.Args {
		if strings.HasPrefix(a, "--kwargs=") {
			return a
		}
	}
	return ""
}
