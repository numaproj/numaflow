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
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
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
}

func (s Source) getContainers(req getContainerReq) ([]corev1.Container, error) {
	containers := []corev1.Container{
		s.getMainContainer(req),
	}
	if s.UDTransformer != nil {
		containers = append(containers, s.getUDTransformerContainer(req))
	}
	return containers, nil
}

func (s Source) getMainContainer(req getContainerReq) corev1.Container {
	return containerBuilder{}.init(req).args("processor", "--type="+string(VertexTypeSource), "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (s Source) getUDTransformerContainer(req getContainerReq) corev1.Container {
	c := containerBuilder{}.
		init(req).
		name(CtrUdtransformer)
	c.Env = nil
	if x := s.UDTransformer.Container; x != nil && x.Image != "" { // customized image
		c = c.image(x.Image)
		if len(x.Command) > 0 {
			c = c.command(x.Command...)
		}
		if len(x.Args) > 0 {
			c = c.args(x.Args...)
		}
		c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources)
	} else { // built-in
		args := []string{"builtin-transformer", "--name=" + s.UDTransformer.Builtin.Name}
		for _, a := range s.UDTransformer.Builtin.Args {
			args = append(args, "--args="+base64.StdEncoding.EncodeToString([]byte(a)))
		}
		var kwargs []string
		for k, v := range s.UDTransformer.Builtin.KWArgs {
			kwargs = append(kwargs, fmt.Sprintf("%s=%s", k, base64.StdEncoding.EncodeToString([]byte(v))))
		}
		if len(kwargs) > 0 {
			// The order of the kwargs items is random because we construct it from an unordered map Builtin.KWArgs.
			// We sort the kwargs first before converting it to a string argument to ensure consistency.
			// This is important because in vertex controller we use hash on PodSpec to determine if a pod already exists, which requires the kwargs being consistent.
			sort.Strings(kwargs)
			args = append(args, "--kwargs="+strings.Join(kwargs, ","))
		}

		c = c.image(req.image).args(args...)
		if x := s.UDTransformer.Container; x != nil {
			c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources)
		}
	}
	return c.build()
}
