package v1alpha1

import (
	"encoding/base64"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type Container struct {
	// +optional
	Image string `json:"image" protobuf:"bytes,1,opt,name=image"`
	// +optional
	Command []string `json:"command,omitempty" protobuf:"bytes,2,rep,name=command"`
	// +optional
	Args []string `json:"args,omitempty" protobuf:"bytes,3,rep,name=args"`
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty" protobuf:"bytes,4,rep,name=env"`
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty" protobuf:"bytes,5,rep,name=volumeMounts"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,6,opt,name=resources"`
}

type Function struct {
	// +kubebuilder:validation:Enum=cat;filter
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// +optional
	Args []string `json:"args,omitempty" protobuf:"bytes,2,rep,name=args"`
	// +optional
	KWArgs map[string]string `json:"kwargs,omitempty" protobuf:"bytes,3,rep,name=kwargs"`
}

type UDF struct {
	// +optional
	Container *Container `json:"container" protobuf:"bytes,1,opt,name=container"`
	// +optional
	Builtin *Function `json:"builtin" protobuf:"bytes,12,opt,name=builtin"`
}

func (in UDF) getContainers(req getContainerReq) ([]corev1.Container, error) {
	return []corev1.Container{in.getMainContainer(req), in.getUDFContainer(req)}, nil
}

func (in UDF) getMainContainer(req getContainerReq) corev1.Container {
	return containerBuilder{}.
		init(req).args("processor", "--type=udf", "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (in UDF) getUDFContainer(req getContainerReq) corev1.Container {
	c := containerBuilder{}.
		init(req).
		name(CtrUdf)
	c.Env = nil
	if x := in.Container; x != nil && x.Image != "" { // customized image
		c = c.image(x.Image)
		if len(x.Command) > 0 {
			c = c.command(x.Command...)
		}
		if len(x.Args) > 0 {
			c = c.args(x.Args...)
		}
		c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources)
	} else { // built-in
		args := []string{"builtin-udf", "--name=" + in.Builtin.Name}
		for _, a := range in.Builtin.Args {
			args = append(args, "--args="+base64.StdEncoding.EncodeToString([]byte(a)))
		}
		var kwargs []string
		for k, v := range in.Builtin.KWArgs {
			kwargs = append(kwargs, fmt.Sprintf("%s=%s", k, base64.StdEncoding.EncodeToString([]byte(v))))
		}
		if len(kwargs) > 0 {
			args = append(args, "--kwargs="+strings.Join(kwargs, ","))
		}

		c = c.image(req.image).args(args...)
		if x := in.Container; x != nil {
			c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources)
		}
	}
	return c.build()
}
