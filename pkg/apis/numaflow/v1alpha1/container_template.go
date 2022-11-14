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

// ContainerTemplate defines customized spec for a container
type ContainerTemplate struct {
	Resources       corev1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,1,opt,name=resources"`
	ImagePullPolicy corev1.PullPolicy           `json:"imagePullPolicy,omitempty" protobuf:"bytes,2,opt,name=imagePullPolicy,casttype=PullPolicy"`
	SecurityContext *corev1.SecurityContext     `json:"securityContext,omitempty" protobuf:"bytes,3,opt,name=securityContext"`
	Env             []corev1.EnvVar             `json:"env,omitempty" protobuf:"bytes,4,rep,name=env"`
}

// ApplyToContainer updates the Container with the values from the ContainerTemplate
func (ct *ContainerTemplate) ApplyToContainer(c *corev1.Container) {
	// currently only doing resources & env, ignoring imagePullPolicy & securityContext
	c.Resources = ct.Resources
	if len(ct.Env) > 0 {
		c.Env = append(c.Env, ct.Env...)
	}
}

// ApplyToNumaflowContainers updates any numa or init containers with the values from the ContainerTemplate
func (ct *ContainerTemplate) ApplyToNumaflowContainers(containers []corev1.Container) {
	for i := range containers {
		if containers[i].Name == CtrMain || containers[i].Name == CtrInit {
			ct.ApplyToContainer(&containers[i])
		}
	}
}
