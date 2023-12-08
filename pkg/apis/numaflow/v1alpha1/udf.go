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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

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
	Builtin *Function `json:"builtin" protobuf:"bytes,2,opt,name=builtin"`
	// +optional
	GroupBy *GroupBy `json:"groupBy" protobuf:"bytes,3,opt,name=groupBy"`
}

func (in UDF) getContainers(req getContainerReq) ([]corev1.Container, error) {
	return []corev1.Container{in.getMainContainer(req), in.getUDFContainer(req)}, nil
}

func (in UDF) getMainContainer(req getContainerReq) corev1.Container {
	if in.GroupBy == nil {
		args := []string{"processor", "--type=" + string(VertexTypeMapUDF), "--isbsvc-type=" + string(req.isbSvcType)}
		return containerBuilder{}.
			init(req).args(args...).build()
	}
	return containerBuilder{}.
		init(req).args("processor", "--type="+string(VertexTypeReduceUDF), "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (in UDF) getUDFContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrUdf).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as main container
		appendVolumeMounts(mainContainerReq.volumeMounts...)
	if x := in.Container; x != nil && x.Image != "" { // customized image
		c = c.image(x.Image)
		if len(x.Command) > 0 {
			c = c.command(x.Command...)
		}
		if len(x.Args) > 0 {
			c = c.args(x.Args...)
		}
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
			// The order of the kwargs items is random because we construct it from an unordered map Builtin.KWArgs.
			// We sort the kwargs first before converting it to a string argument to ensure consistency.
			// This is important because in vertex controller we use hash on PodSpec to determine if a pod already exists, which requires the kwargs being consistent.
			sort.Strings(kwargs)
			args = append(args, "--kwargs="+strings.Join(kwargs, ","))
		}
		c = c.image(mainContainerReq.image).args(args...) // Use the same image as the main container
	}
	if x := in.Container; x != nil {
		c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...)
		if x.ImagePullPolicy != nil {
			c = c.imagePullPolicy(*x.ImagePullPolicy)
		}
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

// GroupBy indicates it is a reducer UDF
type GroupBy struct {
	// Window describes the windowing strategy.
	Window Window `json:"window" protobuf:"bytes,1,opt,name=window"`
	// +optional
	Keyed bool `json:"keyed" protobuf:"bytes,2,opt,name=keyed"`
	// AllowedLateness allows late data to be included for the Reduce operation as long as the late data is not later
	// than (Watermark - AllowedLateness).
	// +optional
	AllowedLateness *metav1.Duration `json:"allowedLateness,omitempty" protobuf:"bytes,3,opt,name=allowedLateness"`
	// Storage is used to define the PBQ storage for a reduce vertex.
	Storage *PBQStorage `json:"storage,omitempty" protobuf:"bytes,4,opt,name=storage"`
}

// Window describes windowing strategy
type Window struct {
	// +optional
	Fixed *FixedWindow `json:"fixed" protobuf:"bytes,1,opt,name=fixed"`
	// +optional
	Sliding *SlidingWindow `json:"sliding" protobuf:"bytes,2,opt,name=sliding"`
	// +optional
	Session *SessionWindow `json:"session" protobuf:"bytes,3,opt,name=session"`
}

// FixedWindow describes a fixed window
type FixedWindow struct {
	Length *metav1.Duration `json:"length,omitempty" protobuf:"bytes,1,opt,name=length"`
	// +optional
	Streaming bool `json:"streaming,omitempty" protobuf:"bytes,3,opt,name=streaming"`
}

// SlidingWindow describes a sliding window
type SlidingWindow struct {
	Length *metav1.Duration `json:"length,omitempty" protobuf:"bytes,1,opt,name=length"`
	Slide  *metav1.Duration `json:"slide,omitempty" protobuf:"bytes,2,opt,name=slide"`
	// +optional
	Streaming bool `json:"streaming,omitempty" protobuf:"bytes,3,opt,name=streaming"`
}

// SessionWindow describes a session window
type SessionWindow struct {
	Timeout *metav1.Duration `json:"timeout,omitempty" protobuf:"bytes,1,opt,name=timeout"`
}

// PBQStorage defines the persistence configuration for a vertex.
type PBQStorage struct {
	// +optional
	PersistentVolumeClaim *PersistenceStrategy `json:"persistentVolumeClaim,omitempty" protobuf:"bytes,1,opt,name=persistentVolumeClaim"`
	// +optional
	EmptyDir *corev1.EmptyDirVolumeSource `json:"emptyDir,omitempty" protobuf:"bytes,2,opt,name=emptyDir"`
}

// GeneratePBQStoragePVCName generates pvc name used by reduce vertex.
func GeneratePBQStoragePVCName(pipelineName, vertex string, index int) string {
	return fmt.Sprintf("pbq-vol-%s-%s-%d", pipelineName, vertex, index)
}
