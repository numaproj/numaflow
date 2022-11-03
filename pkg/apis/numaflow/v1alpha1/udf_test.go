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

func TestUDF_getContainers(t *testing.T) {
	x := UDF{
		Container: &Container{
			Image:        "my-image",
			VolumeMounts: []corev1.VolumeMount{{Name: "my-vm"}},
			Command:      []string{"my-cmd"},
			Args:         []string{"my-arg"},
			Env:          []corev1.EnvVar{{Name: "my-envvar"}},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					"cpu": resource.MustParse("2"),
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
	assert.Equal(t, x.Container.Image, c[1].Image)
	assert.Contains(t, c[1].VolumeMounts, c[1].VolumeMounts[0])
	assert.Equal(t, x.Container.Command, c[1].Command)
	assert.Equal(t, x.Container.Args, c[1].Args)
	assert.Equal(t, x.Container.Env, c[1].Env)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, c[1].Resources)
}

func Test_getUDFContainer(t *testing.T) {
	t.Run("with customized image", func(t *testing.T) {
		x := UDF{
			Container: &Container{
				Image: "my-image",
				Args:  []string{"my-arg"},
			},
		}
		c := x.getUDFContainer(getContainerReq{
			image: "main-image",
		})
		assert.Equal(t, "my-image", c.Image)
		assert.Contains(t, c.Args, "my-arg")
	})

	t.Run("with built-in functions", func(t *testing.T) {
		x := UDF{
			Container: &Container{
				Env: []corev1.EnvVar{
					{Name: "e", Value: "f"},
				},
			},
			Builtin: &Function{
				Name: "cat",
			},
		}
		c := x.getUDFContainer(getContainerReq{
			image: "main-image",
			env: []corev1.EnvVar{
				{Name: "a", Value: "b"},
			},
		})
		assert.Equal(t, "main-image", c.Image)
		assert.Contains(t, c.Args, "--name=cat")
		envNames := []string{}
		for _, e := range c.Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, "e")
		assert.NotContains(t, envNames, "a")
	})
}
