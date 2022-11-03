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

type containerBuilder corev1.Container

func (b containerBuilder) init(req getContainerReq) containerBuilder {
	b.Env = req.env
	b.Image = req.image
	b.ImagePullPolicy = req.imagePullPolicy
	b.Name = CtrMain
	b.Resources = req.resources
	b.VolumeMounts = req.volumeMounts
	return b
}

func (b containerBuilder) args(args ...string) containerBuilder {
	b.Args = args
	return b
}

func (b containerBuilder) image(x string) containerBuilder {
	b.Image = x
	return b
}

func (b containerBuilder) name(x string) containerBuilder {
	b.Name = x
	return b
}

func (b containerBuilder) command(x ...string) containerBuilder {
	b.Command = x
	return b
}

func (b containerBuilder) appendEnv(x ...corev1.EnvVar) containerBuilder {
	b.Env = append(b.Env, x...)
	return b
}

func (b containerBuilder) appendVolumeMounts(x ...corev1.VolumeMount) containerBuilder {
	b.VolumeMounts = append(b.VolumeMounts, x...)
	return b
}

func (b containerBuilder) resources(x corev1.ResourceRequirements) containerBuilder {
	b.Resources = x
	return b
}

func (b containerBuilder) build() corev1.Container {
	return corev1.Container(b)
}
