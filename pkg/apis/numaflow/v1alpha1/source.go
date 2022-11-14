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

type Source struct {
	// +optional
	Generator *GeneratorSource `json:"generator,omitempty" protobuf:"bytes,1,opt,name=generator"`
	// +optional
	Kafka *KafkaSource `json:"kafka,omitempty" protobuf:"bytes,2,opt,name=kafka"`
	// +optional
	HTTP *HTTPSource `json:"http,omitempty" protobuf:"bytes,3,opt,name=http"`
}

func (s Source) getContainers(req getContainerReq) ([]corev1.Container, error) {
	return []corev1.Container{
		containerBuilder{}.init(req).args("processor", "--type="+string(VertexTypeSource), "--isbsvc-type="+string(req.isbSvcType)).build(),
	}, nil
}
