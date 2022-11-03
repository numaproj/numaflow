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
)

type Sink struct {
	Log    *Log       `json:"log,omitempty" protobuf:"bytes,1,opt,name=log"`
	Kafka  *KafkaSink `json:"kafka,omitempty" protobuf:"bytes,2,opt,name=kafka"`
	UDSink *UDSink    `json:"udsink,omitempty" protobuf:"bytes,3,opt,name=udsink"`
}

func (s Sink) getContainers(req getContainerReq) ([]corev1.Container, error) {
	containers := []corev1.Container{
		s.getMainContainer(req),
	}
	if s.UDSink != nil {
		containers = append(containers, s.getUDSinkContainer(req))
	}
	return containers, nil
}

func (s Sink) getMainContainer(req getContainerReq) corev1.Container {
	return containerBuilder{}.init(req).args("processor", "--type="+string(VertexTypeSink), "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (s Sink) getUDSinkContainer(req getContainerReq) corev1.Container {
	c := containerBuilder{}.
		init(req).
		name(CtrUdsink)
	c.Env = nil
	x := s.UDSink.Container
	c = c.image(x.Image)
	if len(x.Command) > 0 {
		c = c.command(x.Command...)
	}
	if len(x.Args) > 0 {
		c = c.args(x.Args...)
	}
	c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources)
	return c.build()
}
