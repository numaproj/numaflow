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
	"fmt"
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

type VertexType string

const (
	VertexTypeSource    VertexType = "Source"
	VertexTypeSink      VertexType = "Sink"
	VertexTypeMapUDF    VertexType = "MapUDF"
	VertexTypeReduceUDF VertexType = "ReduceUDF"
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

func (v Vertex) IsMapUDF() bool {
	return v.Spec.UDF != nil && v.Spec.UDF.GroupBy == nil
}

func (v Vertex) IsReduceUDF() bool {
	return v.Spec.UDF != nil && v.Spec.UDF.GroupBy != nil
}

func (v Vertex) Scalable() bool {
	if v.Spec.Scale.Disabled || v.IsReduceUDF() {
		return false
	}
	if v.IsASink() || v.IsMapUDF() {
		return true
	}
	if v.IsASource() {
		src := v.Spec.Source
		if src.Kafka != nil {
			return true
		}
	}
	return false
}

func (v Vertex) GetHeadlessServiceName() string {
	return v.Name + "-headless"
}

func (v Vertex) GetServiceObjs() []*corev1.Service {
	svcs := []*corev1.Service{v.getServiceObj(v.GetHeadlessServiceName(), true, VertexMetricsPort, VertexMetricsPortName)}
	if x := v.Spec.Source; x != nil && x.HTTP != nil && x.HTTP.Service {
		svcs = append(svcs, v.getServiceObj(v.Name, false, VertexHTTPSPort, VertexHTTPSPortName))
	}
	return svcs
}

func (v Vertex) getServiceObj(name string, headless bool, port int, servicePortName string) *corev1.Service {
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
				{Port: int32(port), TargetPort: intstr.FromInt(port), Name: servicePortName},
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

func (v Vertex) commonEnvs() []corev1.EnvVar {
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
	envVars = append(envVars, v.commonEnvs()...)
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
		containers[1].Env = append(containers[1].Env, v.commonEnvs()...)
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
		InitContainers:     v.getInitContainers(req),
		Containers:         containers,
	}
	return spec, nil
}

func (v Vertex) getInitContainers(req GetVertexPodSpecReq) []corev1.Container {
	envVars := []corev1.EnvVar{
		{Name: EnvPipelineName, Value: v.Spec.PipelineName},
		{Name: "GODEBUG", Value: os.Getenv("GODEBUG")},
	}
	envVars = append(envVars, req.Env...)
	initContainers := []corev1.Container{
		{
			Name:            CtrInit,
			Env:             envVars,
			Image:           req.Image,
			ImagePullPolicy: req.PullPolicy,
			Resources:       standardResources,
			Args:            []string{"isbsvc-buffer-validate", "--isbsvc-type=" + string(req.ISBSvcType)},
		},
	}
	return append(initContainers, v.Spec.InitContainers...)
}
func (vs VertexSpec) WithOutReplicas() VertexSpec {
	zero := int32(0)
	x := *vs.DeepCopy()
	x.Replicas = &zero
	return x
}

type BufferType string

const (
	SourceBuffer BufferType = "so"
	SinkBuffer   BufferType = "si"
	EdgeBuffer   BufferType = "ed"
)

type Buffer struct {
	Name string     `protobuf:"bytes,1,opt,name=name"`
	Type BufferType `protobuf:"bytes,2,opt,name=type,casttype=BufferType"`
}

func (v Vertex) GetFromBuffers() []Buffer {
	r := []Buffer{}
	if v.IsASource() {
		r = append(r, Buffer{GenerateSourceBufferName(v.Namespace, v.Spec.PipelineName, v.Spec.Name), SourceBuffer})
	} else {
		for _, vt := range v.Spec.FromEdges {
			for _, b := range GenerateEdgeBufferNames(v.Namespace, v.Spec.PipelineName, vt) {
				r = append(r, Buffer{b, EdgeBuffer})
			}
		}
	}
	return r
}

func (v Vertex) GetToBuffers() []Buffer {
	r := []Buffer{}
	if v.IsASink() {
		r = append(r, Buffer{GenerateSinkBufferName(v.Namespace, v.Spec.PipelineName, v.Spec.Name), SinkBuffer})
	} else {
		for _, vt := range v.Spec.ToEdges {
			for _, b := range GenerateEdgeBufferNames(v.Namespace, v.Spec.PipelineName, vt) {
				r = append(r, Buffer{b, EdgeBuffer})
			}
		}
	}
	return r
}

func (v Vertex) GetReplicas() int {
	if v.IsReduceUDF() {
		// Replica of a reduce vertex is determined by the parallelism.
		if v.Spec.Replicas != nil && *v.Spec.Replicas == int32(0) {
			// 0 is also allowed, which is for paused pipeline.
			return 0
		}
		return len(v.GetFromBuffers())
	}
	if v.Spec.Replicas == nil {
		return 1
	}
	return int(*v.Spec.Replicas)
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
	FromEdges []Edge `json:"fromEdges,omitempty" protobuf:"bytes,5,rep,name=fromEdges"`
	// +optional
	ToEdges []Edge `json:"toEdges,omitempty" protobuf:"bytes,6,rep,name=toEdges"`
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
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=name
	Volumes []corev1.Volume `json:"volumes,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,16,rep,name=volumes"`
	// Limits define the limitations such as buffer read batch size for all the vertices of a pipeline, will override pipeline level settings
	// +optional
	Limits *VertexLimits `json:"limits,omitempty" protobuf:"bytes,17,opt,name=limits"`
	// Settings for autoscaling
	// +optional
	Scale Scale `json:"scale,omitempty" protobuf:"bytes,18,opt,name=scale"`
	// List of init containers belonging to the pod.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
	// +optional
	InitContainers []corev1.Container `json:"initContainers,omitempty" protobuf:"bytes,19,rep,name=initContainers"`
}

// Scale defines the parameters for autoscaling.
type Scale struct {
	// Whether to disable autoscaling.
	// Set to "true" when using Kubernetes HPA or any other 3rd party autoscaling strategies.
	// +optional
	Disabled bool `json:"disabled,omitempty" protobuf:"bytes,1,opt,name=disabled"`
	// Minimum replicas.
	// +optional
	Min *int32 `json:"min,omitempty" protobuf:"varint,2,opt,name=min"`
	// Maximum replicas.
	// +optional
	Max *int32 `json:"max,omitempty" protobuf:"varint,3,opt,name=max"`
	// Lookback seconds to calculate the average pending messages and processing rate.
	// +optional
	LookbackSeconds *uint32 `json:"lookbackSeconds,omitempty" protobuf:"varint,4,opt,name=lookbackSeconds"`
	// Cooldown seconds after a scaling operation before another one.
	// +optional
	CooldownSeconds *uint32 `json:"cooldownSeconds,omitempty" protobuf:"varint,5,opt,name=cooldownSeconds"`
	// After scaling down to 0, sleep how many seconds before scaling up to peek.
	// +optional
	ZeroReplicaSleepSeconds *uint32 `json:"zeroReplicaSleepSeconds,omitempty" protobuf:"varint,6,opt,name=zeroReplicaSleepSeconds"`
	// TargetProcessingSeconds is used to tune the aggressiveness of autoscaling for source vertices, it measures how fast
	// you want the vertex to process all the pending messages. Typically increasing the value, which leads to lower processing
	// rate, thus less replicas. It's only effective for source vertices.
	// +optional
	TargetProcessingSeconds *uint32 `json:"targetProcessingSeconds,omitempty" protobuf:"varint,7,opt,name=targetProcessingSeconds"`
	// TargetBufferUsage is used to define the target percentage of usage of the buffer to be read.
	// A valid and meaningful value should be less than the BufferUsageLimit defined in the Edge spec (or Pipeline spec), for example, 50.
	// It only applies to UDF and Sink vertices as only they have buffers to read.
	// +optional
	TargetBufferUsage *uint32 `json:"targetBufferUsage,omitempty" protobuf:"varint,8,opt,name=targetBufferUsage"`
	// ReplicasPerScale defines maximum replicas can be scaled up or down at once.
	// The is use to prevent too aggressive scaling operations
	// +optional
	ReplicasPerScale *uint32 `json:"replicasPerScale,omitempty" protobuf:"varint,9,opt,name=replicasPerScale"`
}

func (s Scale) GetLookbackSeconds() int {
	if s.LookbackSeconds != nil {
		return int(*s.LookbackSeconds)
	}
	return DefaultLookbackSeconds
}

func (s Scale) GetCooldownSeconds() int {
	if s.CooldownSeconds != nil {
		return int(*s.CooldownSeconds)
	}
	return DefaultCooldownSeconds
}

func (s Scale) GetZeroReplicaSleepSeconds() int {
	if s.ZeroReplicaSleepSeconds != nil {
		return int(*s.ZeroReplicaSleepSeconds)
	}
	return DefaultZeroReplicaSleepSeconds
}

func (s Scale) GetTargetProcessingSeconds() int {
	if s.TargetProcessingSeconds != nil {
		return int(*s.TargetProcessingSeconds)
	}
	return DefaultTargetProcessingSeconds
}

func (s Scale) GetTargetBufferUsage() int {
	if s.TargetBufferUsage != nil {
		return int(*s.TargetBufferUsage)
	}
	return DefaultTargetBufferUsage
}

func (s Scale) GetReplicasPerScale() int {
	if s.ReplicasPerScale != nil {
		return int(*s.ReplicasPerScale)
	}
	return DefaultReplicasPerScale
}

func (s Scale) GetMinReplicas() int32 {
	if x := s.Min; x == nil || *x < 0 {
		return 0
	} else {
		return *x
	}
}

func (s Scale) GetMaxReplicas() int32 {
	if x := s.Max; x == nil {
		return DefaultMaxReplicas
	} else {
		return *x
	}
}

type VertexLimits struct {
	// Read batch size from the source or buffer.
	// It overrides the settings from pipeline limits.
	// +optional
	ReadBatchSize *uint64 `json:"readBatchSize,omitempty" protobuf:"varint,1,opt,name=readBatchSize"`
	// Read timeout duration from the source or buffer
	// It overrides the settings from pipeline limits.
	// +optional
	ReadTimeout *metav1.Duration `json:"readTimeout,omitempty" protobuf:"bytes,2,opt,name=readTimeout"`
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

// GenerateEdgeBufferNames generates buffer names for an edge
func GenerateEdgeBufferNames(namespace, pipelineName string, edge Edge) []string {
	buffers := []string{}
	if edge.Parallelism == nil {
		buffers = append(buffers, fmt.Sprintf("%s-%s-%s-%s", namespace, pipelineName, edge.From, edge.To))
		return buffers
	}
	for i := int32(0); i < *edge.Parallelism; i++ {
		buffers = append(buffers, fmt.Sprintf("%s-%s-%s-%s-%d", namespace, pipelineName, edge.From, edge.To, i))
	}
	return buffers
}

func GenerateSourceBufferName(namespace, pipelineName, vertex string) string {
	return fmt.Sprintf("%s-%s-%s_SOURCE", namespace, pipelineName, vertex)
}

func GenerateSinkBufferName(namespace, pipelineName, vertex string) string {
	return fmt.Sprintf("%s-%s-%s_SINK", namespace, pipelineName, vertex)
}
