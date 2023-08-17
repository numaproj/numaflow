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
)

var (
	imagePullNever = corev1.PullNever
	testSideInput  = &SideInput{
		Name: "test-side-input",
		Container: &Container{
			Image: "test-image",
			Env: []corev1.EnvVar{
				{Name: "key1", Value: "value1"},
			},
			Command:         []string{"test-command"},
			Args:            []string{"test-args"},
			ImagePullPolicy: &imagePullNever,
		},
		Trigger: &SideInputTrigger{
			Schedule: "@every 1h",
		},
	}

	testGetSideInputDeploymentReq = GetSideInputDeploymentReq{
		ISBSvcType: ISBSvcTypeJetStream,
		Image:      "test-image",
		PullPolicy: corev1.PullAlways,
		Env: []corev1.EnvVar{
			{Name: "key2", Value: "value2"},
		},
	}
)

func Test_getUDContainer(t *testing.T) {
	c := testSideInput.getUDContainer(testGetSideInputDeploymentReq)
	assert.Equal(t, CtrUdSideInput, c.Name)
	assert.Equal(t, testSideInput.Container.Image, c.Image)
	assert.Equal(t, testSideInput.Container.Command, c.Command)
	for _, env := range testSideInput.Container.Env {
		assert.Contains(t, c.Env, env)
	}
	assert.Equal(t, imagePullNever, c.ImagePullPolicy)
}

func Test_getNumaContainer(t *testing.T) {
	c, err := testSideInput.getNumaContainer(*testPipeline, testGetSideInputDeploymentReq)
	assert.NoError(t, err)
	assert.Equal(t, testSideInput.Container.Image, testGetSideInputDeploymentReq.Image)
	for _, env := range testGetSideInputDeploymentReq.Env {
		assert.Contains(t, c.Env, env)
	}
	assert.Equal(t, testGetSideInputDeploymentReq.PullPolicy, c.ImagePullPolicy)
	assert.Equal(t, CtrMain, c.Name)
}

func Test_getInitContainer(t *testing.T) {
	c := testSideInput.getInitContainer(*testPipeline, testGetSideInputDeploymentReq)
	assert.Equal(t, testSideInput.Container.Image, testGetSideInputDeploymentReq.Image)
	for _, env := range testGetSideInputDeploymentReq.Env {
		assert.Contains(t, c.Env, env)
	}
	assert.Equal(t, testGetSideInputDeploymentReq.PullPolicy, c.ImagePullPolicy)
	assert.Equal(t, CtrInit, c.Name)
}

func Test_getManagerDeploymentObj(t *testing.T) {
	newObj := testSideInput.DeepCopy()
	newObj.Volumes = []corev1.Volume{
		{
			Name:         "test-vol",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}
	deploy, err := newObj.getManagerDeploymentObj(*testPipeline, testGetSideInputDeploymentReq)
	assert.NoError(t, err)
	assert.NotNil(t, deploy)
	assert.Equal(t, 1, len(deploy.Spec.Template.Spec.InitContainers))
	assert.Equal(t, 2, len(deploy.Spec.Template.Spec.Containers))
	assert.Equal(t, 2, len(deploy.Spec.Template.Spec.Volumes))
}
