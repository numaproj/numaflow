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
