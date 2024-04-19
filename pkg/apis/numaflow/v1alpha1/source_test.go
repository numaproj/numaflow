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
)

var testImagePullPolicy = corev1.PullNever

func TestSource_getContainers(t *testing.T) {
	x := Source{
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
			},
		},
	}
	c, err := x.getContainers(getContainerReq{
		image: "main-image",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(c))
	assert.Equal(t, "main-image", c[0].Image)
	assert.Equal(t, x.UDTransformer.Container.Image, c[1].Image)
	assert.Contains(t, c[1].VolumeMounts, c[1].VolumeMounts[0])
	assert.Equal(t, x.UDTransformer.Container.Command, c[1].Command)
	assert.Equal(t, x.UDTransformer.Container.Args, c[1].Args)
	envs := map[string]string{}
	for _, e := range c[1].Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, envs[EnvUDContainerType], UDContainerTransformer)
	assert.Equal(t, x.UDTransformer.Container.EnvFrom, c[1].EnvFrom)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, c[1].Resources)
	assert.Equal(t, c[0].ImagePullPolicy, c[1].ImagePullPolicy)
	x.UDTransformer.Container.ImagePullPolicy = &testImagePullPolicy
	c, _ = x.getContainers(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, testImagePullPolicy, c[1].ImagePullPolicy)
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
