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

var (
	testContainerTemplate = &ContainerTemplate{
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				"cpu": resource.MustParse("100m"),
			},
		},
	}
)

func Test_ApplyToContainer(t *testing.T) {
	c := &corev1.Container{}
	testContainerTemplate.ApplyToContainer(c)
	assert.Equal(t, "", string(c.ImagePullPolicy))
	assert.Equal(t, testContainerTemplate.Resources, c.Resources)
	c.Resources.Limits = corev1.ResourceList{
		"cpu": resource.MustParse("200m"),
	}
	testContainerTemplate.ApplyToContainer(c)
	assert.Equal(t, resource.MustParse("200m"), *c.Resources.Limits.Cpu())
	c.Resources.Limits = corev1.ResourceList{
		"memory": resource.MustParse("32Mi"),
	}
	assert.Equal(t, resource.MustParse("32Mi"), *c.Resources.Limits.Memory())
	testContainerTemplate.ImagePullPolicy = corev1.PullAlways
	testContainerTemplate.ApplyToContainer(c)
	assert.Equal(t, corev1.PullAlways, c.ImagePullPolicy)
	c.ImagePullPolicy = corev1.PullIfNotPresent
	testContainerTemplate.ApplyToContainer(c)
	assert.Equal(t, corev1.PullIfNotPresent, c.ImagePullPolicy)
	testContainerTemplate.SecurityContext = &corev1.SecurityContext{}
	testContainerTemplate.ApplyToContainer(c)
	assert.NotNil(t, c.SecurityContext)
	testContainerTemplate.Env = []corev1.EnvVar{{Name: "a", Value: "b"}}
	testContainerTemplate.EnvFrom = []corev1.EnvFromSource{{Prefix: "a", ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "b"}}}}
	testContainerTemplate.ApplyToContainer(c)
	envs := []string{}
	for _, e := range c.Env {
		envs = append(envs, e.Name)
	}
	assert.Contains(t, envs, "a")
	envFroms := []string{}
	for _, e := range c.EnvFrom {
		envFroms = append(envFroms, e.Prefix)
	}
	assert.Contains(t, envFroms, "a")
}

func Test_ApplyToNumaflowContainers(t *testing.T) {
	cs := []corev1.Container{
		{Name: CtrMain},
		{Name: "nono"},
	}
	testContainerTemplate.ApplyToNumaflowContainers(cs)
	assert.Equal(t, testContainerTemplate.Resources, cs[0].Resources)
	assert.NotEqual(t, testContainerTemplate.Resources, cs[1].Resources)
}

func TestApplyProbes(t *testing.T) {
	tests := []struct {
		name     string
		template *ContainerTemplate
		input    *corev1.Container
		expected *corev1.Container
	}{
		{
			name: "Apply ReadinessProbe",
			template: &ContainerTemplate{
				ReadinessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](5),
					TimeoutSeconds:      ptr.To[int32](10),
					PeriodSeconds:       ptr.To[int32](15),
					FailureThreshold:    ptr.To[int32](3),
					SuccessThreshold:    ptr.To[int32](1),
				},
			},
			input: &corev1.Container{
				ReadinessProbe: &corev1.Probe{},
			},
			expected: &corev1.Container{
				ReadinessProbe: &corev1.Probe{
					InitialDelaySeconds: 5,
					TimeoutSeconds:      10,
					PeriodSeconds:       15,
					FailureThreshold:    3,
					SuccessThreshold:    1,
				},
			},
		},
		{
			name: "Apply LivenessProbe",
			template: &ContainerTemplate{
				LivenessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](10),
					TimeoutSeconds:      ptr.To[int32](5),
					PeriodSeconds:       ptr.To[int32](20),
					FailureThreshold:    ptr.To[int32](5),
					SuccessThreshold:    ptr.To[int32](1),
				},
			},
			input: &corev1.Container{
				LivenessProbe: &corev1.Probe{},
			},
			expected: &corev1.Container{
				LivenessProbe: &corev1.Probe{
					InitialDelaySeconds: 10,
					TimeoutSeconds:      5,
					PeriodSeconds:       20,
					FailureThreshold:    5,
					SuccessThreshold:    1,
				},
			},
		},
		{
			name: "Apply Both Probes",
			template: &ContainerTemplate{
				ReadinessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](5),
					TimeoutSeconds:      ptr.To[int32](10),
				},
				LivenessProbe: &Probe{
					PeriodSeconds:    ptr.To[int32](20),
					FailureThreshold: ptr.To[int32](5),
				},
			},
			input: &corev1.Container{
				ReadinessProbe: &corev1.Probe{},
				LivenessProbe:  &corev1.Probe{},
			},
			expected: &corev1.Container{
				ReadinessProbe: &corev1.Probe{
					InitialDelaySeconds: 5,
					TimeoutSeconds:      10,
				},
				LivenessProbe: &corev1.Probe{
					PeriodSeconds:    20,
					FailureThreshold: 5,
				},
			},
		},
		{
			name:     "No Probes in Template",
			template: &ContainerTemplate{},
			input: &corev1.Container{
				ReadinessProbe: &corev1.Probe{InitialDelaySeconds: 30},
				LivenessProbe:  &corev1.Probe{TimeoutSeconds: 15},
			},
			expected: &corev1.Container{
				ReadinessProbe: &corev1.Probe{InitialDelaySeconds: 30},
				LivenessProbe:  &corev1.Probe{TimeoutSeconds: 15},
			},
		},
		{
			name: "No Probes in Container",
			template: &ContainerTemplate{
				ReadinessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](5),
					TimeoutSeconds:      ptr.To[int32](10),
				},
				LivenessProbe: &Probe{
					PeriodSeconds:    ptr.To[int32](20),
					FailureThreshold: ptr.To[int32](5),
				},
			},
			input:    &corev1.Container{},
			expected: &corev1.Container{},
		},
		{
			name: "Partial Probe Updates",
			template: &ContainerTemplate{
				ReadinessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](25),
				},
				LivenessProbe: &Probe{
					FailureThreshold: ptr.To[int32](4),
				},
			},
			input: &corev1.Container{
				ReadinessProbe: &corev1.Probe{TimeoutSeconds: 5},
				LivenessProbe:  &corev1.Probe{PeriodSeconds: 10},
			},
			expected: &corev1.Container{
				ReadinessProbe: &corev1.Probe{
					InitialDelaySeconds: 25,
					TimeoutSeconds:      5,
				},
				LivenessProbe: &corev1.Probe{
					PeriodSeconds:    10,
					FailureThreshold: 4,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.template.ApplyToContainer(tt.input)
			assert.Equal(t, tt.expected.ReadinessProbe, tt.input.ReadinessProbe)
			assert.Equal(t, tt.expected.LivenessProbe, tt.input.LivenessProbe)
		})
	}
}
