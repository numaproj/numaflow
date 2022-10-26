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
