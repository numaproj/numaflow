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
	"os"
	strings "strings"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SideInputs defines information of a Side Input
type SideInput struct {
	Name      string            `json:"name" protobuf:"bytes,1,opt,name=name"`
	Container *Container        `json:"container" protobuf:"bytes,2,opt,name=container"`
	Trigger   *SideInputTrigger `json:"trigger" protobuf:"bytes,3,opt,name=trigger"`
}

type SideInputTrigger struct {
	// +optional
	Schedule *string `json:"schedule" protobuf:"bytes,1,opt,name=schedule"`
	// +optional
	Interval *string `json:"interval" protobuf:"bytes,2,opt,name=interval"`
	// +optional
	Timezone *string `json:"timezone" protobuf:"bytes,3,opt,name=timezone"`
}

func (si SideInput) GetDeploymentSpec(req GetSideInputDeploymentReq) *appv1.Deployment {
	return &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "req.Name",
			Namespace: req.Pipeline.Namespace,
		},
		Spec: appv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: req.Labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      req.Labels,
					Annotations: map[string]string{},
				},
				Spec: corev1.PodSpec{
					Containers:     []corev1.Container{si.getInitContainer(req)},
					InitContainers: []corev1.Container{si.getInitContainer(req)},
				},
			},
		},
	}
}

func (si SideInput) getInitContainer(req GetSideInputDeploymentReq) corev1.Container {
	envVars := []corev1.EnvVar{
		{Name: "GODEBUG", Value: os.Getenv("GODEBUG")},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Name:            CtrInit,
		Env:             envVars,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       standardResources,
		Args:            []string{"isbsvc-validate", "--isbsvc-type=" + string(req.ISBSvcType)},
	}
	c.Args = append(c.Args, "--buffers="+strings.Join(p.GetAllBuffers(), ","))
	c.Args = append(c.Args, "--buckets="+strings.Join(p.GetAllBuckets(), ","))
	if p.Spec.Templates != nil && p.Spec.Templates.DaemonTemplate != nil && p.Spec.Templates.DaemonTemplate.InitContainerTemplate != nil {
		p.Spec.Templates.DaemonTemplate.InitContainerTemplate.ApplyToContainer(&c)
	}
	return c
}
