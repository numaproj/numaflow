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
	"github.com/imdario/mergo"
	corev1 "k8s.io/api/core/v1"
)

// ContainerTemplate defines customized spec for a container
type ContainerTemplate struct {
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,1,opt,name=resources"`
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,2,opt,name=imagePullPolicy,casttype=PullPolicy"`
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty" protobuf:"bytes,3,opt,name=securityContext"`
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty" protobuf:"bytes,4,rep,name=env"`
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty" protobuf:"bytes,5,rep,name=envFrom"`
}

// ApplyToContainer updates the Container with the values from the ContainerTemplate
func (ct *ContainerTemplate) ApplyToContainer(c *corev1.Container) {
	_ = mergo.Merge(&c.Resources, ct.Resources, mergo.WithOverride)
	c.SecurityContext = ct.SecurityContext
	if c.ImagePullPolicy == "" {
		c.ImagePullPolicy = ct.ImagePullPolicy
	}
	if len(ct.Env) > 0 {
		c.Env = append(c.Env, ct.Env...)
	}
	if len(ct.EnvFrom) > 0 {
		c.EnvFrom = append(c.EnvFrom, ct.EnvFrom...)
	}
}

// ApplyToNumaflowContainers updates any numa or init containers with the values from the ContainerTemplate
func (ct *ContainerTemplate) ApplyToNumaflowContainers(containers []corev1.Container) {
	for i := range containers {
		if containers[i].Name == CtrMain || containers[i].Name == CtrInit || containers[i].Name == CtrInitSideInputs {
			ct.ApplyToContainer(&containers[i])
		}
	}
}
