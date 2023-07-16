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
	"encoding/base64"
	"encoding/json"
	"fmt"

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

func (si SideInput) getManagerDeploymentObj(pipeline Pipeline, req GetSideInputDeploymentReq) (*appv1.Deployment, error) {
	numaContainer, err := si.getNumaContainer(pipeline, req)
	if err != nil {
		return nil, err
	}
	labels := map[string]string{
		KeyPartOf:        Project,
		KeyManagedBy:     ControllerPipeline,
		KeyComponent:     ComponentSideInputManager,
		KeyPipelineName:  pipeline.Name,
		KeySideInputName: si.Name,
	}
	return &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipeline.GetSideInputDeploymentName(si.Name),
			Namespace: pipeline.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pipeline.GetObjectMeta(), PipelineGroupVersionKind),
			},
		},
		Spec: appv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						KeyDefaultContainer: CtrUdSideInput,
					},
				},
				Spec: corev1.PodSpec{
					Containers:     []corev1.Container{*numaContainer, si.getUDContainer(req)},
					InitContainers: []corev1.Container{si.getInitContainer(pipeline, req)},
				},
			},
		},
	}, nil
}

func (si SideInput) getInitContainer(pipeline Pipeline, req GetSideInputDeploymentReq) corev1.Container {
	c := corev1.Container{
		Name:            CtrInit,
		Env:             req.Env,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       standardResources,
		Args:            []string{"isbsvc-validate", "--isbsvc-type=" + string(req.ISBSvcType)},
	}
	c.Args = append(c.Args, "--side-inputs-store="+pipeline.GetSideInputsStoreName())
	if x := pipeline.Spec.Templates; x != nil && x.SideInputsManagerTemplate != nil && x.SideInputsManagerTemplate.InitContainerTemplate != nil {
		x.SideInputsManagerTemplate.InitContainerTemplate.ApplyToContainer(&c)
	}
	return c
}

func (si SideInput) getNumaContainer(pipeline Pipeline, req GetSideInputDeploymentReq) (*corev1.Container, error) {
	sideInputCopy := &SideInput{
		Name:    si.Name,
		Trigger: si.Trigger,
	}
	siBytes, err := json.Marshal(sideInputCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SideInput spec")
	}
	encodedSideInput := base64.StdEncoding.EncodeToString(siBytes)
	envVars := []corev1.EnvVar{
		{Name: EnvSideInputObject, Value: encodedSideInput},
	}
	envVars = append(envVars, req.Env...)
	c := &corev1.Container{
		Name:            CtrMain,
		Env:             envVars,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       standardResources,
		Args:            []string{"side-input-manager", "--isbsvc-type=" + string(req.ISBSvcType), "--side-inputs-store=" + pipeline.GetSideInputsStoreName()},
	}
	if x := pipeline.Spec.Templates; x != nil && x.SideInputsManagerTemplate != nil && x.SideInputsManagerTemplate.ContainerTemplate != nil {
		x.SideInputsManagerTemplate.ContainerTemplate.ApplyToContainer(c)
	}
	return c, nil
}

func (si SideInput) getUDContainer(req GetSideInputDeploymentReq) corev1.Container {
	cb := containerBuilder{}.
		name(CtrUdSideInput).
		image(si.Container.Image).
		imagePullPolicy(req.PullPolicy)
	if si.Container.ImagePullPolicy != nil {
		cb = cb.imagePullPolicy(*si.Container.ImagePullPolicy)
	}
	if len(si.Container.Command) > 0 {
		cb = cb.command(si.Container.Command...)
	}
	if len(si.Container.Args) > 0 {
		cb = cb.args(si.Container.Args...)
	}
	// Do not append the envs from req here, as they might contain sensitive information
	cb = cb.appendEnv(si.Container.Env...).appendVolumeMounts(si.Container.VolumeMounts...).
		resources(si.Container.Resources).securityContext(si.Container.SecurityContext).appendEnvFrom(si.Container.EnvFrom...)
	return cb.build()
}
