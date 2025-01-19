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
	// +optional
	ReadinessProbe *Probe `json:"readinessProbe,omitempty" protobuf:"bytes,6,opt,name=readinessProbe"`
	// +optional
	LivenessProbe *Probe `json:"livenessProbe,omitempty" protobuf:"bytes,7,opt,name=livenessProbe"`
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
	if rp := ct.ReadinessProbe; rp != nil && c.ReadinessProbe != nil {
		if rp.InitialDelaySeconds != nil {
			c.ReadinessProbe.InitialDelaySeconds = *rp.InitialDelaySeconds
		}
		if rp.TimeoutSeconds != nil {
			c.ReadinessProbe.TimeoutSeconds = *rp.TimeoutSeconds
		}
		if rp.PeriodSeconds != nil {
			c.ReadinessProbe.PeriodSeconds = *rp.PeriodSeconds
		}
		if rp.FailureThreshold != nil {
			c.ReadinessProbe.FailureThreshold = *rp.FailureThreshold
		}
		if rp.SuccessThreshold != nil {
			c.ReadinessProbe.SuccessThreshold = *rp.SuccessThreshold
		}
	}
	if lp := ct.LivenessProbe; lp != nil && c.LivenessProbe != nil {
		if lp.InitialDelaySeconds != nil {
			c.LivenessProbe.InitialDelaySeconds = *lp.InitialDelaySeconds
		}
		if lp.TimeoutSeconds != nil {
			c.LivenessProbe.TimeoutSeconds = *lp.TimeoutSeconds
		}
		if lp.PeriodSeconds != nil {
			c.LivenessProbe.PeriodSeconds = *lp.PeriodSeconds
		}
		if lp.FailureThreshold != nil {
			c.LivenessProbe.FailureThreshold = *lp.FailureThreshold
		}
		if lp.SuccessThreshold != nil {
			c.LivenessProbe.SuccessThreshold = *lp.SuccessThreshold
		}
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
