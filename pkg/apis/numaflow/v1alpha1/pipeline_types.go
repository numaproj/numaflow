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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// +kubebuilder:validation:Enum="";Running;Failed;Pausing;Paused;Deleting
type PipelinePhase string

const (
	PipelinePhaseUnknown  PipelinePhase = ""
	PipelinePhaseRunning  PipelinePhase = "Running"
	PipelinePhaseFailed   PipelinePhase = "Failed"
	PipelinePhasePausing  PipelinePhase = "Pausing"
	PipelinePhasePaused   PipelinePhase = "Paused"
	PipelinePhaseDeleting PipelinePhase = "Deleting"

	// PipelineConditionConfigured has the status True when the Pipeline
	// has valid configuration.
	PipelineConditionConfigured ConditionType = "Configured"
	// PipelineConditionDeployed has the status True when the Pipeline
	// has its Vertices and Jobs created.
	PipelineConditionDeployed ConditionType = "Deployed"

	// PipelineConditionDaemonServiceHealthy has the status True when the daemon service is healthy.
	PipelineConditionDaemonServiceHealthy      ConditionType = "DaemonServiceHealthy"
	PipelineConditionSideInputsManagersHealthy ConditionType = "SideInputsManagersHealthy"
	PipelineConditionVerticesHealthy           ConditionType = "VerticesHealthy"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pl
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Vertices",type=integer,JSONPath=`.status.vertexCount`
// +kubebuilder:printcolumn:name="Sources",type=integer,JSONPath=`.status.sourceCount`,priority=10
// +kubebuilder:printcolumn:name="Sinks",type=integer,JSONPath=`.status.sinkCount`,priority=10
// +kubebuilder:printcolumn:name="UDFs",type=integer,JSONPath=`.status.udfCount`,priority=10
// +kubebuilder:printcolumn:name="Map UDFs",type=integer,JSONPath=`.status.mapUDFCount`,priority=10
// +kubebuilder:printcolumn:name="Reduce UDFs",type=integer,JSONPath=`.status.reduceUDFCount`,priority=10
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec PipelineSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// +optional
	Status PipelineStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// GetVertex is used to find the AbstractVertex info from vertex name.
func (p Pipeline) GetVertex(vertexName string) *AbstractVertex {
	for _, v := range p.Spec.Vertices {
		if vertexName == v.Name {
			return &v
		}
	}
	return nil
}

// ListAllEdges returns a copy of all the edges.
func (p Pipeline) ListAllEdges() []Edge {
	edges := []Edge{}
	for _, e := range p.Spec.Edges {
		edgeCopy := e.DeepCopy()
		edges = append(edges, *edgeCopy)
	}
	return edges
}

// NumOfPartitions returns the number of partitions for a vertex.
func (p Pipeline) NumOfPartitions(vertex string) int {
	v := p.GetVertex(vertex)
	if v == nil {
		return 0
	}
	partitions := 1
	if v.Partitions != nil {
		partitions = v.GetPartitionCount()
	}
	return partitions
}

func (p Pipeline) FindVertexWithBuffer(buffer string) *AbstractVertex {
	for _, v := range p.Spec.Vertices {
		for _, b := range v.OwnedBufferNames(p.Namespace, p.Name) {
			if buffer == b {
				return &v
			}
		}
	}
	return nil
}

func (p Pipeline) GetToEdges(vertexName string) []Edge {
	edges := []Edge{}
	for _, e := range p.ListAllEdges() {
		if e.From == vertexName {
			edges = append(edges, *e.DeepCopy())
		}
	}
	return edges
}

func (p Pipeline) GetFromEdges(vertexName string) []Edge {
	edges := []Edge{}
	for _, e := range p.ListAllEdges() {
		if e.To == vertexName {
			edges = append(edges, *e.DeepCopy())
		}
	}
	return edges
}

func (p Pipeline) GetAllBuffers() []string {
	r := []string{}
	for _, v := range p.Spec.Vertices {
		r = append(r, v.OwnedBufferNames(p.Namespace, p.Name)...)
	}
	return r
}

func (p Pipeline) GetAllBuckets() []string {
	r := []string{}
	for _, e := range p.ListAllEdges() {
		r = append(r, GenerateEdgeBucketName(p.Namespace, p.Name, e.From, e.To))
	}
	for _, v := range p.Spec.Vertices {
		if v.Source != nil {
			r = append(r, GenerateSourceBucketName(p.Namespace, p.Name, v.Name))
		} else if v.Sink != nil {
			r = append(r, GenerateSinkBucketName(p.Namespace, p.Name, v.Name))
		}
	}
	return r
}

// GetDownstreamEdges returns all the downstream edges of a vertex
func (p Pipeline) GetDownstreamEdges(vertexName string) []Edge {
	var f func(vertexName string, edges *[]Edge, visited map[string]bool)
	f = func(vertexName string, edges *[]Edge, visited map[string]bool) {
		if visited[vertexName] {
			return
		}
		visited[vertexName] = true
		for _, b := range p.ListAllEdges() {
			if b.From == vertexName {
				*edges = append(*edges, b)
				f(b.To, edges, visited)
			}
		}
	}
	result := []Edge{}
	visited := make(map[string]bool)
	f(vertexName, &result, visited)
	return result
}

// HasSideInputs returns if the pipeline has side inputs.
func (p Pipeline) HasSideInputs() bool {
	return len(p.Spec.SideInputs) > 0
}

func (p Pipeline) GetDaemonServiceName() string {
	return fmt.Sprintf("%s-daemon-svc", p.Name)
}

func (p Pipeline) GetDaemonDeploymentName() string {
	return fmt.Sprintf("%s-daemon", p.Name)
}

func (p Pipeline) GetDaemonServiceURL() string {
	// Note: the format of the URL is also used in `server/apis/v1/handler.go`
	// Do not change it without updating the handler.
	return fmt.Sprintf("%s.%s.svc:%d", p.GetDaemonServiceName(), p.Namespace, DaemonServicePort)
}

func (p Pipeline) GetSideInputsManagerDeploymentName(sideInputName string) string {
	return fmt.Sprintf("%s-si-%s", p.Name, sideInputName)
}

func (p Pipeline) GetSideInputsStoreName() string {
	return fmt.Sprintf("%s-%s", p.Namespace, p.Name)
}

func (p Pipeline) GetServingSourceStreamNames() []string {
	var servingSourceNames []string
	for _, srcVertex := range p.Spec.Vertices {
		if srcVertex.IsASource() && srcVertex.Source.Serving != nil {
			servingSourceNames = append(servingSourceNames, fmt.Sprintf("%s-%s-serving-source", p.Name, srcVertex.Name))
		}
	}
	return servingSourceNames
}

func (p Pipeline) GetSideInputsManagerDeployments(req GetSideInputDeploymentReq) ([]*appv1.Deployment, error) {
	commonEnvVars := []corev1.EnvVar{
		{Name: EnvNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: EnvPod, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
	}
	deployments := []*appv1.Deployment{}
	for _, sideInput := range p.Spec.SideInputs {
		deployment, err := sideInput.getManagerDeploymentObj(p, req)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(deployment.Spec.Template.Spec.Containers); i++ {
			deployment.Spec.Template.Spec.Containers[i].Env = append(deployment.Spec.Template.Spec.Containers[i].Env, commonEnvVars...)
		}
		deployment.Spec.Template.Spec.InitContainers[0].Env = append(deployment.Spec.Template.Spec.InitContainers[0].Env, corev1.EnvVar{Name: EnvGoDebug, Value: os.Getenv(EnvGoDebug)})
		deployment.Spec.Template.Spec.Containers[0].Env = append(deployment.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: EnvGoDebug, Value: os.Getenv(EnvGoDebug)})
		deployments = append(deployments, deployment)
	}
	return deployments, nil
}

func (p Pipeline) GetDaemonDeploymentObj(req GetDaemonDeploymentReq) (*appv1.Deployment, error) {
	pipelineCopy := &Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: p.Namespace,
			Name:      p.Name,
		},
		Spec: p.Spec,
	}
	pipelineCopy.Spec.Lifecycle = Lifecycle{}
	plBytes, err := json.Marshal(pipelineCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pipeline spec")
	}
	encodedPipeline := base64.StdEncoding.EncodeToString(plBytes)
	envVars := []corev1.EnvVar{
		{Name: EnvNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: EnvPod, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: EnvPipelineObject, Value: encodedPipeline},
		{Name: EnvGoDebug, Value: os.Getenv(EnvGoDebug)},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Ports:           []corev1.ContainerPort{{ContainerPort: DaemonServicePort}},
		Name:            CtrMain,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       req.DefaultResources,
		Env:             envVars,
		Args:            []string{"daemon-server", "--isbsvc-type=" + string(req.ISBSvcType)},
	}

	c.ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/readyz",
				Port:   intstr.FromInt32(DaemonServicePort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 3,
		PeriodSeconds:       3,
		TimeoutSeconds:      1,
	}
	c.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/livez",
				Port:   intstr.FromInt32(DaemonServicePort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}

	labels := map[string]string{
		KeyPartOf:       Project,
		KeyManagedBy:    ControllerPipeline,
		KeyComponent:    ComponentDaemon,
		KeyAppName:      p.GetDaemonDeploymentName(),
		KeyPipelineName: p.Name,
	}
	spec := appv1.DeploymentSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      labels,
				Annotations: map[string]string{},
			},
			Spec: corev1.PodSpec{
				Containers:     []corev1.Container{c},
				InitContainers: []corev1.Container{p.getDaemonPodInitContainer(req)},
			},
		},
	}
	if p.Spec.Templates != nil && p.Spec.Templates.DaemonTemplate != nil {
		dt := p.Spec.Templates.DaemonTemplate
		spec.Replicas = dt.Replicas
		dt.AbstractPodTemplate.ApplyToPodTemplateSpec(&spec.Template)
		if dt.ContainerTemplate != nil {
			dt.ContainerTemplate.ApplyToNumaflowContainers(spec.Template.Spec.Containers)
		}
	}
	return &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: p.Namespace,
			Name:      p.GetDaemonDeploymentName(),
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(p.GetObjectMeta(), PipelineGroupVersionKind),
			},
		},
		Spec: spec,
	}, nil
}

func (p Pipeline) getDaemonPodInitContainer(req GetDaemonDeploymentReq) corev1.Container {
	envVars := []corev1.EnvVar{
		{Name: EnvGoDebug, Value: os.Getenv(EnvGoDebug)},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Name:            CtrInit,
		Env:             envVars,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       req.DefaultResources,
		Args:            []string{"isbsvc-validate", "--isbsvc-type=" + string(req.ISBSvcType)},
	}
	c.Args = append(c.Args, "--buffers="+strings.Join(p.GetAllBuffers(), ","))
	c.Args = append(c.Args, "--buckets="+strings.Join(p.GetAllBuckets(), ","))
	if p.Spec.Templates != nil && p.Spec.Templates.DaemonTemplate != nil && p.Spec.Templates.DaemonTemplate.InitContainerTemplate != nil {
		p.Spec.Templates.DaemonTemplate.InitContainerTemplate.ApplyToContainer(&c)
	}
	return c
}

func (p Pipeline) GetDaemonServiceObj() *corev1.Service {
	labels := map[string]string{
		KeyPartOf:       Project,
		KeyManagedBy:    ControllerPipeline,
		KeyComponent:    ComponentDaemon,
		KeyPipelineName: p.Name,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: p.Namespace,
			Name:      p.GetDaemonServiceName(),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(p.GetObjectMeta(), PipelineGroupVersionKind),
			},
			Labels: labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "tcp", Port: DaemonServicePort, TargetPort: intstr.FromInt32(DaemonServicePort)},
			},
			Selector: labels,
		},
	}
}

// GetPipelineLimits returns the pipeline limits with default values
func (p Pipeline) GetPipelineLimits() PipelineLimits {
	defaultReadBatchSize := uint64(DefaultReadBatchSize)
	defaultBufferMaxLength := uint64(DefaultBufferLength)
	defaultBufferUsageLimit := uint32(100 * DefaultBufferUsageLimit)
	defaultReadTimeout := DefaultReadTimeout
	limits := PipelineLimits{
		ReadBatchSize:    &defaultReadBatchSize,
		BufferMaxLength:  &defaultBufferMaxLength,
		BufferUsageLimit: &defaultBufferUsageLimit,
		ReadTimeout:      &metav1.Duration{Duration: defaultReadTimeout},
	}
	if x := p.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			limits.ReadBatchSize = x.ReadBatchSize
		}
		if x.BufferMaxLength != nil {
			limits.BufferMaxLength = x.BufferMaxLength
		}
		if x.BufferUsageLimit != nil {
			limits.BufferUsageLimit = x.BufferUsageLimit
		}
		if x.ReadTimeout != nil {
			limits.ReadTimeout = x.ReadTimeout
		}
	}
	return limits
}

type Lifecycle struct {
	// DeletionGracePeriodSeconds used to delete pipeline gracefully
	// +kubebuilder:default=30
	// +optional
	DeletionGracePeriodSeconds *int64 `json:"deletionGracePeriodSeconds,omitempty" protobuf:"varint,1,opt,name=deletionGracePeriodSeconds"`
	// DesiredPhase used to bring the pipeline from current phase to desired phase
	// +kubebuilder:default=Running
	// +optional
	DesiredPhase PipelinePhase `json:"desiredPhase,omitempty" protobuf:"bytes,2,opt,name=desiredPhase"`
	// PauseGracePeriodSeconds used to pause pipeline gracefully
	// +kubebuilder:default=30
	// +optional
	PauseGracePeriodSeconds *int64 `json:"pauseGracePeriodSeconds,omitempty" protobuf:"varint,3,opt,name=pauseGracePeriodSeconds"`
	// DeleteGracePeriodSeconds used to delete pipeline gracefully
	// +kubebuilder:default=30
	// Deprecated: Use DeletionGracePeriodSeconds instead
	// +optional
	DeprecatedDeleteGracePeriodSeconds *int64 `json:"deleteGracePeriodSeconds,omitempty" protobuf:"varint,4,opt,name=deleteGracePeriodSeconds"`
}

// GetTerminationGracePeriodSeconds returns the value DeleteGracePeriodSeconds.
func (p Pipeline) GetTerminationGracePeriodSeconds() int64 {
	if p.Spec.Lifecycle.DeletionGracePeriodSeconds != nil {
		return *p.Spec.Lifecycle.DeletionGracePeriodSeconds
	}
	if p.Spec.Lifecycle.DeprecatedDeleteGracePeriodSeconds != nil {
		return *p.Spec.Lifecycle.DeprecatedDeleteGracePeriodSeconds
	}
	if p.DeletionGracePeriodSeconds != nil {
		return *p.DeletionGracePeriodSeconds
	}
	return 30
}

func (p Pipeline) GetDesiredPhase() PipelinePhase {
	if string(p.Spec.Lifecycle.DesiredPhase) != "" {
		return p.Spec.Lifecycle.DesiredPhase
	}
	return PipelinePhaseRunning
}

// return PauseGracePeriodSeconds if set
func (p Pipeline) GetPauseGracePeriodSeconds() int64 {
	if p.Spec.Lifecycle.PauseGracePeriodSeconds != nil {
		return *p.Spec.Lifecycle.PauseGracePeriodSeconds
	}
	return 30
}

type PipelineSpec struct {
	// +optional
	InterStepBufferServiceName string `json:"interStepBufferServiceName,omitempty" protobuf:"bytes,1,opt,name=interStepBufferServiceName"`
	// +patchStrategy=merge
	// +patchMergeKey=name
	Vertices []AbstractVertex `json:"vertices,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,2,rep,name=vertices"`
	// Edges define the relationships between vertices
	Edges []Edge `json:"edges,omitempty" protobuf:"bytes,3,rep,name=edges"`
	// Lifecycle define the Lifecycle properties
	// +kubebuilder:default={"deleteGracePeriodSeconds": 30, "desiredPhase": Running, "pauseGracePeriodSeconds": 30}
	// +optional
	Lifecycle Lifecycle `json:"lifecycle,omitempty" protobuf:"bytes,4,opt,name=lifecycle"`
	// Limits define the limitations such as buffer read batch size for all the vertices of a pipeline, they could be overridden by each vertex's settings
	// +kubebuilder:default={"readBatchSize": 500, "bufferMaxLength": 30000, "bufferUsageLimit": 80}
	// +optional
	Limits *PipelineLimits `json:"limits,omitempty" protobuf:"bytes,5,opt,name=limits"`
	// Watermark enables watermark progression across the entire pipeline.
	// +kubebuilder:default={"disabled": false}
	// +optional
	Watermark Watermark `json:"watermark,omitempty" protobuf:"bytes,6,opt,name=watermark"`
	// Templates are used to customize additional kubernetes resources required for the Pipeline
	// +optional
	Templates *Templates `json:"templates,omitempty" protobuf:"bytes,7,opt,name=templates"`
	// SideInputs defines the Side Inputs of a pipeline.
	// +optional
	SideInputs []SideInput `json:"sideInputs,omitempty" protobuf:"bytes,8,rep,name=sideInputs"`
}

func (pipeline PipelineSpec) GetMatchingVertices(f func(AbstractVertex) bool) map[string]*AbstractVertex {
	mappedVertices := make(map[string]*AbstractVertex)
	for i := range pipeline.Vertices {
		v := &pipeline.Vertices[i]
		if f(*v) {
			mappedVertices[v.Name] = v
		}
	}
	return mappedVertices
}

func (pipeline PipelineSpec) GetVerticesByName() map[string]*AbstractVertex {
	return pipeline.GetMatchingVertices(func(v AbstractVertex) bool {
		return true
	})
}

func (pipeline PipelineSpec) GetSourcesByName() map[string]*AbstractVertex {
	return pipeline.GetMatchingVertices(func(v AbstractVertex) bool {
		return v.IsASource()
	})
}

func (pipeline PipelineSpec) GetSinksByName() map[string]*AbstractVertex {
	return pipeline.GetMatchingVertices(func(v AbstractVertex) bool {
		return v.IsASink()
	})
}

type Watermark struct {
	// Disabled toggles the watermark propagation, defaults to false.
	// +kubebuilder:default=false
	// +optional
	Disabled bool `json:"disabled,omitempty" protobuf:"bytes,1,opt,name=disabled"`
	// Maximum delay allowed for watermark calculation, defaults to "0s", which means no delay.
	// +kubebuilder:default="0s"
	// +optional
	MaxDelay *metav1.Duration `json:"maxDelay,omitempty" protobuf:"bytes,2,opt,name=maxDelay"`
	// IdleSource defines the idle watermark properties, it could be configured in case source is idling.
	// +optional
	IdleSource *IdleSource `json:"idleSource,omitempty" protobuf:"bytes,3,opt,name=idleSource"`
}

// GetMaxDelay returns the configured max delay with a default value.
func (wm Watermark) GetMaxDelay() time.Duration {
	if wm.MaxDelay != nil {
		return wm.MaxDelay.Duration
	}
	return time.Duration(0)
}

type IdleSource struct {
	// Threshold is the duration after which a source is marked as Idle due to lack of data.
	// Ex: If watermark found to be idle after the Threshold duration then the watermark is progressed by `IncrementBy`.
	Threshold *metav1.Duration `json:"threshold,omitempty" protobuf:"bytes,1,opt,name=threshold"`
	// StepInterval is the duration between the subsequent increment of the watermark as long the source remains Idle.
	// The default value is 0s which means that once we detect idle source, we will be incrementing the watermark by
	// `IncrementBy` for time we detect that we source is empty (in other words, this will be a very frequent update).
	// +kubebuilder:default="0s"
	// +optional
	StepInterval *metav1.Duration `json:"stepInterval,omitempty" protobuf:"bytes,2,opt,name=stepInterval"`
	// IncrementBy is the duration to be added to the current watermark to progress the watermark when source is idling.
	IncrementBy *metav1.Duration `json:"incrementBy,omitempty" protobuf:"bytes,3,opt,name=incrementBy"`
}

func (is IdleSource) GetThreshold() time.Duration {
	if is.Threshold != nil {
		return is.Threshold.Duration
	}

	return time.Duration(0)
}

func (is IdleSource) GetIncrementBy() time.Duration {
	if is.IncrementBy != nil {
		return is.IncrementBy.Duration
	}

	return time.Duration(0)
}

func (is IdleSource) GetStepInterval() time.Duration {
	if is.StepInterval != nil {
		return is.StepInterval.Duration
	}
	return time.Duration(0)
}

type Templates struct {
	// DaemonTemplate is used to customize the Daemon Deployment.
	// +optional
	DaemonTemplate *DaemonTemplate `json:"daemon,omitempty" protobuf:"bytes,1,opt,name=daemon"`
	// JobTemplate is used to customize Jobs.
	// +optional
	JobTemplate *JobTemplate `json:"job,omitempty" protobuf:"bytes,2,opt,name=job"`
	// SideInputsManagerTemplate is used to customize the Side Inputs Manager.
	// +optional
	SideInputsManagerTemplate *SideInputsManagerTemplate `json:"sideInputsManager,omitempty" protobuf:"bytes,3,opt,name=sideInputsManager"`
	// VertexTemplate is used to customize the vertices of the pipeline.
	// +optional
	VertexTemplate *VertexTemplate `json:"vertex,omitempty" protobuf:"bytes,4,opt,name=vertex"`
}

type PipelineLimits struct {
	// Read batch size for all the vertices in the pipeline, can be overridden by the vertex's limit settings.
	// +kubebuilder:default=500
	// +optional
	ReadBatchSize *uint64 `json:"readBatchSize,omitempty" protobuf:"varint,1,opt,name=readBatchSize"`
	// BufferMaxLength is used to define the max length of a buffer.
	// Only applies to UDF and Source vertices as only they do buffer write.
	// It can be overridden by the settings in vertex limits.
	// +kubebuilder:default=30000
	// +optional
	BufferMaxLength *uint64 `json:"bufferMaxLength,omitempty" protobuf:"varint,2,opt,name=bufferMaxLength"`
	// BufferUsageLimit is used to define the percentage of the buffer usage limit, a valid value should be less than 100, for example, 85.
	// Only applies to UDF and Source vertices as only they do buffer write.
	// It will be overridden by the settings in vertex limits.
	// +kubebuilder:default=80
	// +optional
	BufferUsageLimit *uint32 `json:"bufferUsageLimit,omitempty" protobuf:"varint,3,opt,name=bufferUsageLimit"`
	// Read timeout for all the vertices in the pipeline, can be overridden by the vertex's limit settings
	// +kubebuilder:default= "1s"
	// +optional
	ReadTimeout *metav1.Duration `json:"readTimeout,omitempty" protobuf:"bytes,4,opt,name=readTimeout"`
}

type PipelineStatus struct {
	Status `json:",inline" protobuf:"bytes,1,opt,name=status"`
	// +optional
	Phase PipelinePhase `json:"phase,omitempty" protobuf:"bytes,2,opt,name=phase,casttype=PipelinePhase"`
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// +optional
	LastUpdated metav1.Time `json:"lastUpdated,omitempty" protobuf:"bytes,4,opt,name=lastUpdated"`
	// +optional
	VertexCount *uint32 `json:"vertexCount,omitempty" protobuf:"varint,5,opt,name=vertexCount"`
	// +optional
	SourceCount *uint32 `json:"sourceCount,omitempty" protobuf:"varint,6,opt,name=sourceCount"`
	// +optional
	SinkCount *uint32 `json:"sinkCount,omitempty" protobuf:"varint,7,opt,name=sinkCount"`
	// +optional
	UDFCount *uint32 `json:"udfCount,omitempty" protobuf:"varint,8,opt,name=udfCount"`
	// +optional
	MapUDFCount *uint32 `json:"mapUDFCount,omitempty" protobuf:"varint,9,opt,name=mapUDFCount"`
	// +optional
	ReduceUDFCount *uint32 `json:"reduceUDFCount,omitempty" protobuf:"varint,10,opt,name=reduceUDFCount"`
	// The generation observed by the Pipeline controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,11,opt,name=observedGeneration"`
	// Field to indicate if a pipeline drain successfully occurred, only meaningful when the pipeline is paused.
	// True means it has been successfully drained.
	// +optional
	DrainedOnPause bool `json:"drainedOnPause,omitempty" protobuf:"bytes,12,opt,name=drainedOnPause"`
}

// SetVertexCounts sets the counts of vertices.
func (pls *PipelineStatus) SetVertexCounts(vertices []AbstractVertex) {
	var vertexCount = uint32(len(vertices))
	var sinkCount uint32
	var sourceCount uint32
	var udfCount uint32
	var mapUDFCount uint32
	var reduceUDFCount uint32
	for _, v := range vertices {
		if v.IsASource() {
			sourceCount++
		}
		if v.IsASink() {
			sinkCount++
		}
		if v.IsMapUDF() {
			mapUDFCount++
			udfCount++
		}
		if v.IsReduceUDF() {
			reduceUDFCount++
			udfCount++
		}
	}

	pls.VertexCount = &vertexCount
	pls.SinkCount = &sinkCount
	pls.SourceCount = &sourceCount
	pls.UDFCount = &udfCount
	pls.MapUDFCount = &mapUDFCount
	pls.ReduceUDFCount = &reduceUDFCount
}

func (pls *PipelineStatus) SetPhase(phase PipelinePhase, msg string) {
	pls.Phase = phase
	pls.Message = msg
}

// InitConditions sets conditions to Unknown state.
func (pls *PipelineStatus) InitConditions() {
	pls.InitializeConditions(PipelineConditionConfigured, PipelineConditionDeployed, PipelineConditionDaemonServiceHealthy,
		PipelineConditionSideInputsManagersHealthy, PipelineConditionVerticesHealthy)
}

// MarkConfigured set the Pipeline has valid configuration.
func (pls *PipelineStatus) MarkConfigured() {
	pls.MarkTrue(PipelineConditionConfigured)
}

// MarkNotConfigured the Pipeline has configuration.
func (pls *PipelineStatus) MarkNotConfigured(reason, message string) {
	pls.MarkFalse(PipelineConditionConfigured, reason, message)
	pls.SetPhase(PipelinePhaseFailed, message)
}

// MarkDeployed set the Pipeline has been deployed.
func (pls *PipelineStatus) MarkDeployed() {
	pls.MarkTrue(PipelineConditionDeployed)
}

// MarkDeployFailed set the Pipeline deployment failed
func (pls *PipelineStatus) MarkDeployFailed(reason, message string) {
	pls.MarkFalse(PipelineConditionDeployed, reason, message)
	pls.SetPhase(PipelinePhaseFailed, message)
}

// MarkDaemonServiceHealthy set the daemon service of the pipeline is healthy.
func (pls *PipelineStatus) MarkDaemonServiceHealthy() {
	pls.MarkTrue(PipelineConditionDaemonServiceHealthy)
}

// MarkDaemonServiceUnHealthy set the daemon service of the pipeline is unhealthy.
func (pls *PipelineStatus) MarkDaemonServiceUnHealthy(reason, message string) {
	pls.MarkFalse(PipelineConditionDaemonServiceHealthy, reason, message)
	pls.Message = "Degraded: " + message
}

// MarkSideInputsManagersHealthy set the Side Inputs managers of the pipeline are healthy.
func (pls *PipelineStatus) MarkSideInputsManagersHealthy() {
	pls.MarkTrue(PipelineConditionSideInputsManagersHealthy)
}

// MarkSideInputsManagersHealthyWithReason set the Side Inputs managers of the pipeline are healthy with the given reason.
func (pls *PipelineStatus) MarkSideInputsManagersHealthyWithReason(reason, message string) {
	pls.MarkTrueWithReason(PipelineConditionSideInputsManagersHealthy, reason, message)
}

// MarkSideInputsManagersUnHealthy set the Side Inputs managers of the pipeline are unhealthy.
func (pls *PipelineStatus) MarkSideInputsManagersUnHealthy(reason, message string) {
	pls.MarkFalse(PipelineConditionSideInputsManagersHealthy, reason, message)
	pls.Message = "Degraded: " + message
}

// MarkVerticesHealthy set the vertices of the pipeline are healthy.
func (pls *PipelineStatus) MarkVerticesHealthy() {
	pls.MarkTrueWithReason(PipelineConditionVerticesHealthy, "Successful", "All vertices are healthy")
}

// MarkVerticesUnHealthy set the vertices of the pipeline are unhealthy with the given reason.
func (pls *PipelineStatus) MarkVerticesUnHealthy(reason, message string) {
	pls.MarkFalse(PipelineConditionVerticesHealthy, reason, message)
	pls.Message = "Degraded: " + message
}

// MarkPhaseRunning set the Pipeline has been running.
func (pls *PipelineStatus) MarkPhaseRunning() {
	pls.SetPhase(PipelinePhaseRunning, "")
}

// MarkPhasePaused set the Pipeline has been paused.
func (pls *PipelineStatus) MarkPhasePaused() {
	pls.SetPhase(PipelinePhasePaused, "Pipeline paused")
}

// MarkPhasePausing set the Pipeline is pausing.
func (pls *PipelineStatus) MarkPhasePausing() {
	pls.SetPhase(PipelinePhasePausing, "Pausing in progress")
}

// MarkPhaseDeleting set the Pipeline is deleting.
func (pls *PipelineStatus) MarkPhaseDeleting() {
	pls.SetPhase(PipelinePhaseDeleting, "Deleting in progress")
}

// SetObservedGeneration sets the Status ObservedGeneration
func (pls *PipelineStatus) SetObservedGeneration(value int64) {
	pls.ObservedGeneration = value
}

// MarkDrainedOnPauseTrue sets the DrainedOnPause field to true
func (pls *PipelineStatus) MarkDrainedOnPauseTrue() {
	pls.DrainedOnPause = true
}

// MarkDrainedOnPauseFalse sets the DrainedOnPause field to false
func (pls *PipelineStatus) MarkDrainedOnPauseFalse() {
	pls.DrainedOnPause = false
}

// IsHealthy indicates whether the pipeline is in healthy status
func (pls *PipelineStatus) IsHealthy() bool {
	switch pls.Phase {
	case PipelinePhaseFailed:
		return false
	case PipelinePhaseRunning, PipelinePhasePaused:
		return pls.IsReady()
	case PipelinePhaseDeleting, PipelinePhasePausing:
		// Transient phases, return true
		return true
	default:
		return false
	}
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []Pipeline `json:"items" protobuf:"bytes,2,rep,name=items"`
}
