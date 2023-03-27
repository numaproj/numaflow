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
