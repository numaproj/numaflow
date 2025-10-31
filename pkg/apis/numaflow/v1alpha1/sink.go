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
	// OnSuccess sink allows triggering a secondary sink operation only after the primary sink completes successfully
	// The writes to OnSuccess sink will only be initiated if the ud-sink response field sets it.
	// A new Message crafted in the Primary sink can be written on the OnSuccess sink.
	// +optional
	OnSuccess *AbstractSink `json:"onSuccess,omitempty" protobuf:"bytes,3,opt,name=onSuccess"`
	// RetryStrategy struct encapsulates the settings for retrying operations in the event of failures.
	// +optional
	RetryStrategy RetryStrategy `json:"retryStrategy,omitempty" protobuf:"bytes,4,opt,name=retryStrategy"`
}

type AbstractSink struct {
	// Log sink is used to write the data to the log.
	// +optional
	Log *Log `json:"log,omitempty" protobuf:"bytes,1,opt,name=log"`
	// Kafka sink is used to write the data to the Kafka.
	// +optional
	Kafka *KafkaSink `json:"kafka,omitempty" protobuf:"bytes,2,opt,name=kafka"`
	// Blackhole sink is used to write the data to the blackhole sink,
	// which is a sink that discards all the data written to it.
	// +optional
	Blackhole *Blackhole `json:"blackhole,omitempty" protobuf:"bytes,3,opt,name=blackhole"`
	// UDSink sink is used to write the data to the user-defined sink.
	// +optional
	UDSink *UDSink `json:"udsink,omitempty" protobuf:"bytes,4,opt,name=udsink"`
	// Serve sink is used to return results when working with a ServingPipeline.
	// +optional
	Serve *ServeSink `json:"serve,omitempty" protobuf:"bytes,5,opt,name=serve"`
	// SQS sink is used to write the data to the AWS SQS.
	// +optional
	Sqs *SqsSink `json:"sqs,omitempty" protobuf:"bytes,6,opt,name=sqs"`
	// Pulsar sink is used to write the data to the Apache Pulsar.
	// +optional
	Pulsar *PulsarSink `json:"pulsar,omitempty" protobuf:"bytes,7,opt,name=pulsar"`
}

func (s Sink) getContainers(req getContainerReq) ([]corev1.Container, []corev1.Container, error) {
	containers := []corev1.Container{
		s.getMainContainer(req),
	}
	monitorContainer := buildMonitorContainer(req)
	sidecarContainers := []corev1.Container{monitorContainer}
	if s.UDSink != nil {
		sidecarContainers = append(sidecarContainers, s.getUDSinkContainer(req))
	}
	if s.Fallback != nil && s.Fallback.UDSink != nil {
		sidecarContainers = append(sidecarContainers, s.getFallbackUDSinkContainer(req))
	}
	if s.OnSuccess != nil && s.OnSuccess.UDSink != nil {
		sidecarContainers = append(sidecarContainers, s.getOnSuccessUDSinkContainer(req))
	}
	return sidecarContainers, containers, nil
}

func (s Sink) getMainContainer(req getContainerReq) corev1.Container {
	// TODO: Default runtime is rust in 1.6, we will remove this env in 1.7
	req.env = append(req.env, corev1.EnvVar{Name: EnvNumaflowRuntime, Value: "rust"})
	return containerBuilder{}.init(req).args("processor", "--type="+string(VertexTypeSink), "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (s Sink) getUDSinkContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrUdsink).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...).asSidecar()
	x := s.UDSink.Container
	c = c.image(x.Image)
	if len(x.Command) > 0 {
		c = c.command(x.Command...)
	}
	if len(x.Args) > 0 {
		c = c.args(x.Args...)
	}
	c = c.appendEnv(corev1.EnvVar{Name: EnvUDContainerType, Value: UDContainerSink})
	c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...).appendPorts(x.Ports...)
	if x.ImagePullPolicy != nil {
		c = c.imagePullPolicy(*x.ImagePullPolicy)
	}
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt32(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: GetProbeInitialDelaySecondsOr(x.LivenessProbe, UDContainerLivezInitialDelaySeconds),
		PeriodSeconds:       GetProbePeriodSecondsOr(x.LivenessProbe, UDContainerLivezPeriodSeconds),
		TimeoutSeconds:      GetProbeTimeoutSecondsOr(x.LivenessProbe, UDContainerLivezTimeoutSeconds),
		FailureThreshold:    GetProbeFailureThresholdOr(x.LivenessProbe, UDContainerLivezFailureThreshold),
	}
	return container
}

func (s Sink) getFallbackUDSinkContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrFallbackUdsink).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...).asSidecar()
	x := s.Fallback.UDSink.Container
	c = c.image(x.Image)
	if len(x.Command) > 0 {
		c = c.command(x.Command...)
	}
	if len(x.Args) > 0 {
		c = c.args(x.Args...)
	}
	c = c.appendEnv(corev1.EnvVar{Name: EnvUDContainerType, Value: UDContainerFallbackSink})
	c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...).appendPorts(x.Ports...)
	if x.ImagePullPolicy != nil {
		c = c.imagePullPolicy(*x.ImagePullPolicy)
	}
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt32(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: GetProbeInitialDelaySecondsOr(x.LivenessProbe, UDContainerLivezInitialDelaySeconds),
		PeriodSeconds:       GetProbePeriodSecondsOr(x.LivenessProbe, UDContainerLivezPeriodSeconds),
		TimeoutSeconds:      GetProbeTimeoutSecondsOr(x.LivenessProbe, UDContainerLivezTimeoutSeconds),
		FailureThreshold:    GetProbeFailureThresholdOr(x.LivenessProbe, UDContainerLivezFailureThreshold),
	}
	return container
}

func (s Sink) getOnSuccessUDSinkContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrOnSuccessUdsink).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...).asSidecar()
	x := s.OnSuccess.UDSink.Container
	c = c.image(x.Image)
	if len(x.Command) > 0 {
		c = c.command(x.Command...)
	}
	if len(x.Args) > 0 {
		c = c.args(x.Args...)
	}
	c = c.appendEnv(corev1.EnvVar{Name: EnvUDContainerType, Value: UDContainerOnSuccessSink})
	c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...).appendPorts(x.Ports...)
	if x.ImagePullPolicy != nil {
		c = c.imagePullPolicy(*x.ImagePullPolicy)
	}
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt32(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: GetProbeInitialDelaySecondsOr(x.LivenessProbe, UDContainerLivezInitialDelaySeconds),
		PeriodSeconds:       GetProbePeriodSecondsOr(x.LivenessProbe, UDContainerLivezPeriodSeconds),
		TimeoutSeconds:      GetProbeTimeoutSecondsOr(x.LivenessProbe, UDContainerLivezTimeoutSeconds),
		FailureThreshold:    GetProbeFailureThresholdOr(x.LivenessProbe, UDContainerLivezFailureThreshold),
	}
	return container
}

// IsAnySinkSpecified returns true if any sink is specified.
func (a *AbstractSink) IsAnySinkSpecified() bool {
	return a.Log != nil || a.Kafka != nil || a.Blackhole != nil || a.UDSink != nil || a.Sqs != nil || a.Pulsar != nil
}
