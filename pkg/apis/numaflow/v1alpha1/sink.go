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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type Sink struct {
	AbstractSink `json:",inline" protobuf:"bytes,1,opt,name=abstractSink"`
	// Fallback sink can be imagined as DLQ for primary Sink. The writes to Fallback sink will only be
	// initiated if the ud-sink response field sets it.
	// +optional
	Fallback *AbstractSink `json:"fallback,omitempty" protobuf:"bytes,2,opt,name=fallback"`
}

type AbstractSink struct {
	Log       *Log       `json:"log,omitempty" protobuf:"bytes,1,opt,name=log"`
	Kafka     *KafkaSink `json:"kafka,omitempty" protobuf:"bytes,2,opt,name=kafka"`
	Blackhole *Blackhole `json:"blackhole,omitempty" protobuf:"bytes,3,opt,name=blackhole"`
	UDSink    *UDSink    `json:"udsink,omitempty" protobuf:"bytes,4,opt,name=udsink"`
}

func (s Sink) getContainers(req getContainerReq) ([]corev1.Container, error) {
	containers := []corev1.Container{
		s.getMainContainer(req),
	}
	if s.UDSink != nil {
		containers = append(containers, s.getUDSinkContainer(req))
	}
	if s.Fallback != nil && s.Fallback.UDSink != nil {
		containers = append(containers, s.getFallbackUDSinkContainer(req))
	}
	return containers, nil
}

func (s Sink) getMainContainer(req getContainerReq) corev1.Container {
	return containerBuilder{}.init(req).args("processor", "--type="+string(VertexTypeSink), "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (s Sink) getUDSinkContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrUdsink).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...)
	x := s.UDSink.Container
	c = c.image(x.Image)
	if len(x.Command) > 0 {
		c = c.command(x.Command...)
	}
	if len(x.Args) > 0 {
		c = c.args(x.Args...)
	}
	c = c.appendEnv(corev1.EnvVar{Name: EnvUDContainerType, Value: UDContainerSink})
	c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...)
	if x.ImagePullPolicy != nil {
		c = c.imagePullPolicy(*x.ImagePullPolicy)
	}
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}
	return container
}

func (s Sink) getFallbackUDSinkContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrFallbackUdsink).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...)
	x := s.Fallback.UDSink.Container
	c = c.image(x.Image)
	if len(x.Command) > 0 {
		c = c.command(x.Command...)
	}
	if len(x.Args) > 0 {
		c = c.args(x.Args...)
	}
	c = c.appendEnv(corev1.EnvVar{Name: EnvUDContainerType, Value: UDContainerFallbackSink})
	c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...)
	if x.ImagePullPolicy != nil {
		c = c.imagePullPolicy(*x.ImagePullPolicy)
	}
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}
	return container
}
