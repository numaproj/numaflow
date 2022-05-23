/*
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
	"encoding/json"
	"errors"
	fmt "fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// +kubebuilder:validation:Enum="";Pending;Running;Succeeded;Failed
type VertexPhase string

const (
	VertexPhaseUnknown   VertexPhase = ""
	VertexPhasePending   VertexPhase = "Pending"
	VertexPhaseRunning   VertexPhase = "Running"
	VertexPhaseSucceeded VertexPhase = "Succeeded"
	VertexPhaseFailed    VertexPhase = "Failed"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=vtx
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +kubebuilder:printcolumn:name="Desired",type=string,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Current",type=string,JSONPath=`.status.replicas`
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type Vertex struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec VertexSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// +optional
	Status VertexStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

func (v Vertex) IsASource() bool {
	return v.Spec.Source != nil
}

func (v Vertex) IsASink() bool {
	return v.Spec.Sink != nil
}

func (v Vertex) IsAnUDF() bool {
	return v.Spec.Sink == nil && v.Spec.Source == nil
}

func (v Vertex) GetHeadlessServiceName() string {
	return v.Name + "-headless"
}

func (v Vertex) GetServiceObjs() []*corev1.Service {
	svcs := []*corev1.Service{v.getServiceObj(v.GetHeadlessServiceName(), true, VertexMetricsPort)}
	if x := v.Spec.Source; x != nil && x.HTTP != nil && x.HTTP.Service {
		svcs = append(svcs, v.getServiceObj(v.Name, false, VertexHTTPSPort))
	}
	return svcs
}

func (v Vertex) getServiceObj(name string, headless bool, port int) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       v.Namespace,
			Name:            name,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(v.GetObjectMeta(), VertexGroupVersionKind)},
			Labels: map[string]string{
				KeyPartOf:       Project,
				KeyManagedBy:    ControllerVertex,
				KeyComponent:    ComponentVertex,
				KeyVertexName:   v.Spec.Name,
				KeyPipelineName: v.Spec.PipelineName,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Port: int32(port), TargetPort: intstr.FromInt(port)},
			},
			Selector: map[string]string{
				KeyPartOf:       Project,
				KeyManagedBy:    ControllerVertex,
				KeyComponent:    ComponentVertex,
				KeyVertexName:   v.Spec.Name,
				KeyPipelineName: v.Spec.PipelineName,
			},
		},
	}
	if headless {
		svc.Spec.ClusterIP = "None"
	}
	return svc
}

func (v Vertex) commonEvns() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: EnvNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: EnvPod, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: EnvReplica, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + KeyReplica + "']"}}},
		{Name: EnvPipelineName, Value: v.Spec.PipelineName},
		{Name: EnvVertexName, Value: v.Spec.Name},
	}
}

func (v Vertex) GetPodSpec(req GetVertexPodSpecReq) (*corev1.PodSpec, error) {
	vertexCopy := &Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: v.Namespace,
			Name:      v.Name,
		},
		Spec: v.Spec.WithOutReplicas(),
	}
	vertexBytes, err := json.Marshal(vertexCopy)
	if err != nil {
		return nil, errors.New("failed to marshal vertex spec")
	}
	encodedVertexSpec := base64.StdEncoding.EncodeToString(vertexBytes)
	envVars := []corev1.EnvVar{
		{Name: EnvVertexObject, Value: encodedVertexSpec},
	}
	envVars = append(envVars, v.commonEvns()...)
	envVars = append(envVars, req.Env...)
	resources := standardResources
	if v.Spec.ContainerTemplate != nil {
		resources = v.Spec.ContainerTemplate.Resources
		if len(v.Spec.ContainerTemplate.Env) > 0 {
			envVars = append(envVars, v.Spec.ContainerTemplate.Env...)
		}
	}

	varVolumeName := "var-run-numaflow"
	volumes := []corev1.Volume{
		{
			Name: varVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium: corev1.StorageMediumMemory,
			}},
		},
	}
	volumeMounts := []corev1.VolumeMount{{Name: varVolumeName, MountPath: PathVarRun}}

	containers, err := v.Spec.getType().getContainers(getContainerReq{
		isbSvcType:      req.ISBSvcType,
		env:             envVars,
		image:           req.Image,
		imagePullPolicy: req.PullPolicy,
		resources:       resources,
		volumeMounts:    volumeMounts,
	})
	if err != nil {
		return nil, err
	}
	containers[0].ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/readyz",
				Port:   intstr.FromInt(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 3,
		PeriodSeconds:       3,
		TimeoutSeconds:      1,
	}
	containers[0].LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/healthz",
				Port:   intstr.FromInt(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 3,
		PeriodSeconds:       3,
		TimeoutSeconds:      1,
	}

	if len(containers) > 1 { // udf and udsink
		containers[1].Env = append(containers[1].Env, v.commonEvns()...)
	}

	spec := &corev1.PodSpec{
		Subdomain:          v.GetHeadlessServiceName(),
		NodeSelector:       v.Spec.NodeSelector,
		Tolerations:        v.Spec.Tolerations,
		SecurityContext:    v.Spec.SecurityContext,
		ImagePullSecrets:   v.Spec.ImagePullSecrets,
		PriorityClassName:  v.Spec.PriorityClassName,
		Priority:           v.Spec.Priority,
		Affinity:           v.Spec.Affinity,
		ServiceAccountName: v.Spec.ServiceAccountName,
		Volumes:            append(volumes, v.Spec.Volumes...),
		InitContainers: []corev1.Container{
			v.getInitContainer(req),
		},
		Containers: containers,
	}
	return spec, nil
}

func (v Vertex) getInitContainer(req GetVertexPodSpecReq) corev1.Container {
	envVars := []corev1.EnvVar{
		{Name: EnvPipelineName, Value: v.Spec.PipelineName},
		{Name: "GODEBUG", Value: os.Getenv("GODEBUG")},
	}
	envVars = append(envVars, req.Env...)
	return corev1.Container{
		Name:            CtrInit,
		Env:             envVars,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       standardResources,
		Args:            []string{"isbsvc-buffer-validate", "--isbsvc-type=" + string(req.ISBSvcType)},
	}
}

func (v Vertex) GetFromBuffers() []string {
	r := []string{}
	for _, vt := range v.Spec.FromVertices {
		r = append(r, GenerateBufferName(v.Namespace, v.Spec.PipelineName, vt, v.Spec.Name))
	}
	return r
}

func (v Vertex) GetToBuffers() []string {
	r := []string{}
	for _, vt := range v.Spec.ToVertices {
		r = append(r, GenerateBufferName(v.Namespace, v.Spec.PipelineName, v.Spec.Name, vt.Name))
	}
	return r
}

func (v Vertex) GetToBufferName(toVertexName string) string {
	return GenerateBufferName(v.Namespace, v.Spec.PipelineName, v.Spec.Name, toVertexName)
}

type VertexSpec struct {
	AbstractVertex `json:",inline" protobuf:"bytes,1,opt,name=abstractVertex"`
	PipelineName   string `json:"pipelineName" protobuf:"bytes,2,opt,name=pipelineName"`
	// +optional
	InterStepBufferServiceName string `json:"interStepBufferServiceName" protobuf:"bytes,3,opt,name=interStepBufferServiceName"`
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,4,opt,name=replicas"`
	// +optional
	FromVertices []string `json:"fromVertices,omitempty" protobuf:"bytes,5,rep,name=fromVertices"`
	// +optional
	ToVertices []ToVertex `json:"toVertices,omitempty" protobuf:"bytes,6,rep,name=toVertices"`
}

type ToVertex struct {
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// +optional
	Conditions *ForwardConditions `json:"conditions" protobuf:"bytes,2,opt,name=conditions"`
}

func (vs VertexSpec) WithOutReplicas() VertexSpec {
	zero := int32(0)
	x := *vs.DeepCopy()
	x.Replicas = &zero
	return x
}

func (vs VertexSpec) GetReplicas() int {
	if vs.Replicas == nil {
		return 1
	}
	return int(*vs.Replicas)
}

type AbstractVertex struct {
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// +optional
	Source *Source `json:"source,omitempty" protobuf:"bytes,3,rep,name=source"`
	// +optional
	Sink *Sink `json:"sink,omitempty" protobuf:"bytes,4,rep,name=sink"`
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,5,rep,name=containerTemplate"`
	// +optional
	UDF *UDF `json:"udf,omitempty" protobuf:"bytes,6,rep,name=udf"`
	// Metadata sets the pods's metadata, i.e. annotations and labels
	Metadata *Metadata `json:"metadata,omitempty" protobuf:"bytes,7,opt,name=metadata"`
	// NodeSelector is a selector which must be true for the pod to fit on a node.
	// Selector which must match a node's labels for the pod to be scheduled on that node.
	// More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,8,rep,name=nodeSelector"`
	// If specified, the pod's tolerations.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty" protobuf:"bytes,9,rep,name=tolerations"`
	// SecurityContext holds pod-level security attributes and common container settings.
	// Optional: Defaults to empty.  See type description for default values of each field.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty" protobuf:"bytes,10,opt,name=securityContext"`
	// ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.
	// If specified, these secrets will be passed to individual puller implementations for them to use. For example,
	// in the case of docker, only DockerConfig type secrets are honored.
	// More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,11,rep,name=imagePullSecrets"`
	// If specified, indicates the Redis pod's priority. "system-node-critical"
	// and "system-cluster-critical" are two special keywords which indicate the
	// highest priorities with the former being the highest priority. Any other
	// name must be defined by creating a PriorityClass object with that name.
	// If not specified, the pod priority will be default or zero if there is no
	// default.
	// More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,12,opt,name=priorityClassName"`
	// The priority value. Various system components use this field to find the
	// priority of the Redis pod. When Priority Admission Controller is enabled,
	// it prevents users from setting this field. The admission controller populates
	// this field from PriorityClassName.
	// The higher the value, the higher the priority.
	// More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/
	// +optional
	Priority *int32 `json:"priority,omitempty" protobuf:"bytes,13,opt,name=priority"`
	// The pod's scheduling constraints
	// More info: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty" protobuf:"bytes,14,opt,name=affinity"`
	// ServiceAccountName to apply to the StatefulSet
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" protobuf:"bytes,15,opt,name=serviceAccountName"`
	// +patchStrategy=merge
	// +patchMergeKey=name
	Volumes []corev1.Volume `json:"volumes,omitempty" protobuf:"bytes,16,rep,name=volumes"`
	// Limits define the limitations such as buffer read batch size for all the vertices of a pipleine, will override pipeline level settings
	// +optional
	Limits *VertexLimits `json:"limits,omitempty" protobuf:"bytes,17,opt,name=limits"`
}

type VertexLimits struct {
	// Read batch size
	// +optional
	ReadBatchSize *uint64 `json:"readBatchSize,omitempty" protobuf:"varint,1,opt,name=readBatchSize"`
	// Workers used to concurrently call UDF functions, it's only meaningful for UDF vertex, and will be ignored by source and sink vertices.
	// It overrides the setting in pipeline limits.
	// +optional
	UDFWorkers *uint32 `json:"udfWorkers,omitempty" protobuf:"varint,2,opt,name=udfWorkers"`
	// BufferMaxLength is used to define the max length of a buffer.
	// It overrides the settings from pipeline limits.
	// Only meaningful for UDF and Source vertice as only they do buffer write.
	// +optional
	BufferMaxLength *uint64 `json:"bufferMaxLength,omitempty" protobuf:"varint,3,opt,name=bufferMaxLength"`
	// BufferUsageLimit is used to define the pencentage of the buffer usage limit, a valid value should be less than 100, for example, 85.
	// It overrides the settings from pipeline limits.
	// Only meaningful for UDF and Source vertice as only they do buffer write.
	// +optional
	BufferUsageLimit *uint32 `json:"bufferUsageLimit,omitempty" protobuf:"varint,4,opt,name=bufferUsageLimit"`
}

func (v VertexSpec) getType() containerSupplier {
	if x := v.Source; x != nil {
		return x
	} else if x := v.Sink; x != nil {
		return x
	} else if x := v.UDF; x != nil {
		return x
	} else {
		panic("invalid vertex spec")
	}
}

type VertexStatus struct {
	Phase        VertexPhase `json:"phase" protobuf:"bytes,1,opt,name=phase,casttype=VertexPhase"`
	Reason       string      `json:"reason,omitempty" protobuf:"bytes,6,opt,name=reason"`
	Message      string      `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
	Replicas     uint32      `json:"replicas" protobuf:"varint,3,opt,name=replicas"`
	Selector     string      `json:"selector,omitempty" protobuf:"bytes,5,opt,name=selector"`
	LastScaledAt metav1.Time `json:"lastScaledAt,omitempty" protobuf:"bytes,4,opt,name=lastScaledAt"`
}

func (vs *VertexStatus) MarkPhase(phase VertexPhase, reason, message string) {
	vs.Phase = phase
	vs.Reason = reason
	vs.Message = message
}

func (vs *VertexStatus) MarkPhaseFailed(reason, message string) {
	vs.MarkPhase(VertexPhaseFailed, reason, message)
}

func (vs *VertexStatus) MarkPhaseRunning() {
	vs.MarkPhase(VertexPhaseRunning, "", "")
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type VertexList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []Vertex `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// GenerateBufferName generates buffer name
func GenerateBufferName(namespace, pipelineName, fromVetex, toVertex string) string {
	return fmt.Sprintf("%s-%s-%s-%s", namespace, pipelineName, fromVetex, toVertex)
}
