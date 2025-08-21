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

type Source struct {
	// +optional
	Generator *GeneratorSource `json:"generator,omitempty" protobuf:"bytes,1,opt,name=generator"`
	// +optional
	Kafka *KafkaSource `json:"kafka,omitempty" protobuf:"bytes,2,opt,name=kafka"`
	// +optional
	HTTP *HTTPSource `json:"http,omitempty" protobuf:"bytes,3,opt,name=http"`
	// +optional
	Nats *NatsSource `json:"nats,omitempty" protobuf:"bytes,4,opt,name=nats"`
	// +optional
	UDTransformer *UDTransformer `json:"transformer,omitempty" protobuf:"bytes,5,opt,name=transformer"`
	// +optional
	UDSource *UDSource `json:"udsource,omitempty" protobuf:"bytes,6,opt,name=udSource"`
	// +optional
	JetStream *JetStreamSource `json:"jetstream,omitempty" protobuf:"bytes,7,opt,name=jetstream"`
	// +optional
	Serving *ServingSource `json:"serving,omitempty" protobuf:"bytes,8,opt,name=serving"`
	// +optional
	Pulsar *PulsarSource `json:"pulsar,omitempty" protobuf:"bytes,9,opt,name=pulsar"`
	// +optional
	Sqs *SqsSource `json:"sqs,omitempty" protobuf:"bytes,10,opt,name=sqs"`
}

func (s Source) getContainers(req getContainerReq) ([]corev1.Container, []corev1.Container, error) {
	containers := []corev1.Container{
		s.getMainContainer(req),
	}
	monitorContainer := buildMonitorContainer(req)
	sidecarContainers := []corev1.Container{monitorContainer}
	if s.UDTransformer != nil {
		sidecarContainers = append(sidecarContainers, s.getUDTransformerContainer(req))
	}
	if s.UDSource != nil {
		sidecarContainers = append(sidecarContainers, s.getUDSourceContainer(req))
	}
	return sidecarContainers, containers, nil
}

func (s Source) getMainContainer(req getContainerReq) corev1.Container {
	// TODO: Default runtime is rust in 1.6, we will remove this env in 1.7
	req.env = append(req.env, corev1.EnvVar{Name: EnvNumaflowRuntime, Value: "rust"})
	return containerBuilder{}.init(req).args("processor", "--type="+string(VertexTypeSource), "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (s Source) getUDTransformerContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrUdtransformer).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...).asSidecar()
	c = c.appendEnv(corev1.EnvVar{Name: EnvUDContainerType, Value: UDContainerTransformer})
	// At this point, x (the UDTransformer.Container) is guaranteed to be non-nil and x.Image is guaranteed to be a non-empty string due to prior validation.
	x := s.UDTransformer.Container
	c = c.image(x.Image)
	if len(x.Command) > 0 {
		c = c.command(x.Command...)
	}
	if len(x.Args) > 0 {
		c = c.args(x.Args...)
	}
	c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...).appendPorts(x.Ports...)
	if x.ImagePullPolicy != nil {
		c = c.imagePullPolicy(*x.ImagePullPolicy)
	}

	container := c.build()

	var initialDelaySeconds, periodSeconds, timeoutSeconds, failureThreshold int32 = UDContainerLivezInitialDelaySeconds, UDContainerLivezPeriodSeconds, UDContainerLivezTimeoutSeconds, UDContainerLivezFailureThreshold
	initialDelaySeconds = GetProbeInitialDelaySecondsOr(x.LivenessProbe, initialDelaySeconds)
	periodSeconds = GetProbePeriodSecondsOr(x.LivenessProbe, periodSeconds)
	timeoutSeconds = GetProbeTimeoutSecondsOr(x.LivenessProbe, timeoutSeconds)
	failureThreshold = GetProbeFailureThresholdOr(x.LivenessProbe, failureThreshold)

	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt32(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: initialDelaySeconds,
		PeriodSeconds:       periodSeconds,
		TimeoutSeconds:      timeoutSeconds,
		FailureThreshold:    failureThreshold,
	}
	return container
}

func (s Source) getUDSourceContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrUdsource).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...).asSidecar()
	c = c.appendEnv(corev1.EnvVar{Name: EnvUDContainerType, Value: UDContainerSource})
	if x := s.UDSource.Container; x != nil && x.Image != "" { // customized image
		c = c.image(x.Image)
		if len(x.Command) > 0 {
			c = c.command(x.Command...)
		}
		if len(x.Args) > 0 {
			c = c.args(x.Args...)
		}
	}
	if x := s.UDSource.Container; x != nil {
		c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...).appendPorts(x.Ports...)
		if x.ImagePullPolicy != nil {
			c = c.imagePullPolicy(*x.ImagePullPolicy)
		}
	}
	container := c.build()

	var initialDelaySeconds, periodSeconds, timeoutSeconds, failureThreshold int32 = UDContainerLivezInitialDelaySeconds, UDContainerLivezPeriodSeconds, UDContainerLivezTimeoutSeconds, UDContainerLivezFailureThreshold
	if x := s.UDSource.Container; x != nil {
		initialDelaySeconds = GetProbeInitialDelaySecondsOr(x.LivenessProbe, initialDelaySeconds)
		periodSeconds = GetProbePeriodSecondsOr(x.LivenessProbe, periodSeconds)
		timeoutSeconds = GetProbeTimeoutSecondsOr(x.LivenessProbe, timeoutSeconds)
		failureThreshold = GetProbeFailureThresholdOr(x.LivenessProbe, failureThreshold)
	}
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt32(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: initialDelaySeconds,
		PeriodSeconds:       periodSeconds,
		TimeoutSeconds:      timeoutSeconds,
		FailureThreshold:    failureThreshold,
	}
	return container
}
