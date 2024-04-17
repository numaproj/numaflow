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

func Test_Sink_getContainers(t *testing.T) {
	s := Sink{}
	c, err := s.getContainers(getContainerReq{
		env: []corev1.EnvVar{
			{Name: "test-env", Value: "test-val"},
		},
		isbSvcType:      ISBSvcTypeJetStream,
		imagePullPolicy: corev1.PullIfNotPresent,
		image:           testFlowImage,
		resources:       corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c))
	assert.Equal(t, testFlowImage, c[0].Image)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, c[0].Resources)
}

func Test_Sink_getUDSinkContainer(t *testing.T) {
	x := Sink{
		AbstractSink: AbstractSink{
			UDSink: &UDSink{
				Container: Container{
					Image:           "my-image",
					Args:            []string{"my-arg"},
					SecurityContext: &corev1.SecurityContext{},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
					}}},
				},
			},
		},
	}
	c := x.getUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, CtrUdsink, c.Name)
	assert.NotNil(t, c.SecurityContext)
	assert.Equal(t, corev1.PullAlways, c.ImagePullPolicy)
	assert.Equal(t, "my-image", c.Image)
	assert.Contains(t, c.Args, "my-arg")
	assert.Equal(t, 1, len(c.EnvFrom))
	envs := map[string]string{}
	for _, e := range c.Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, envs[EnvUDContainerType], UDContainerSink)
	x.UDSink.Container.ImagePullPolicy = &testImagePullPolicy
	c = x.getUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, testImagePullPolicy, c.ImagePullPolicy)
	assert.True(t, c.LivenessProbe != nil)
}

func Test_Sink_getFallbackUDSinkContainer(t *testing.T) {
	x := Sink{
		AbstractSink: AbstractSink{
			UDSink: &UDSink{
				Container: Container{
					Image:           "my-image",
					Args:            []string{"my-arg"},
					SecurityContext: &corev1.SecurityContext{},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
					}}},
				},
			},
		},
		Fallback: &AbstractSink{
			UDSink: &UDSink{
				Container: Container{
					Image:           "my-image",
					Args:            []string{"my-arg"},
					SecurityContext: &corev1.SecurityContext{},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
					}}},
				},
			},
		},
	}
	c := x.getFallbackUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, CtrFallbackUdsink, c.Name)
	assert.NotNil(t, c.SecurityContext)
	assert.Equal(t, corev1.PullAlways, c.ImagePullPolicy)
	assert.Equal(t, "my-image", c.Image)
	assert.Contains(t, c.Args, "my-arg")
	assert.Equal(t, 1, len(c.EnvFrom))
	envs := map[string]string{}
	for _, e := range c.Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, envs[EnvUDContainerType], UDContainerFallbackSink)
	x.UDSink.Container.ImagePullPolicy = &testImagePullPolicy
	c = x.getUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, testImagePullPolicy, c.ImagePullPolicy)
	assert.True(t, c.LivenessProbe != nil)
}
