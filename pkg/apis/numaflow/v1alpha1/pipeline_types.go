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
	"fmt"
	"os"
	"strings"
	"time"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

// +kubebuilder:validation:Enum="";Running;Succeeded;Failed;Pausing;Paused;Deleting
type PipelinePhase string

const (
	PipelinePhaseUnknown   PipelinePhase = ""
	PipelinePhaseRunning   PipelinePhase = "Running"
	PipelinePhaseSucceeded PipelinePhase = "Succeeded"
	PipelinePhaseFailed    PipelinePhase = "Failed"
	PipelinePhasePausing   PipelinePhase = "Pausing"
	PipelinePhasePaused    PipelinePhase = "Paused"
	PipelinePhaseDeleting  PipelinePhase = "Deleting"

	// PipelineConditionConfigured has the status True when the Pipeline
	// has valid configuration.
	PipelineConditionConfigured ConditionType = "Configured"
	// PipelineConditionDeployed has the status True when the Pipeline
	// has its Vertices and Jobs created.
	PipelineConditionDeployed ConditionType = "Deployed"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=pl
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +kubebuilder:printcolumn:name="Vertices",type=integer,JSONPath=`.status.vertexCount`
// +kubebuilder:printcolumn:name="Sources",type=integer,JSONPath=`.status.sourceCount`,priority=10
// +kubebuilder:printcolumn:name="Sinks",type=integer,JSONPath=`.status.sinkCount`,priority=10
// +kubebuilder:printcolumn:name="UDFs",type=integer,JSONPath=`.status.udfCount`,priority=10
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
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
		toVertex := p.GetVertex(e.To)
		if toVertex.UDF == nil || toVertex.UDF.GroupBy == nil {
			// Clean up parallelism if downstream vertex is not a reduce UDF.
			edgeCopy.Parallelism = nil
		} else if edgeCopy.Parallelism == nil || *edgeCopy.Parallelism < 1 {
			// Set parallelism = 1 if it's not set.
			edgeCopy.Parallelism = pointer.Int32(1)
		}
		edges = append(edges, *edgeCopy)
	}
	return edges
}

// FindEdgeWithBuffer is used to locate the edge of the buffer.
func (p Pipeline) FindEdgeWithBuffer(buffer string) *Edge {
	for _, e := range p.ListAllEdges() {
		for _, b := range GenerateEdgeBufferNames(p.Namespace, p.Name, e) {
			if buffer == b {
				return &e
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

func (p Pipeline) GetAllBuffers() []Buffer {
	r := []Buffer{}
	for _, e := range p.ListAllEdges() {
		for _, b := range GenerateEdgeBufferNames(p.Namespace, p.Name, e) {
			r = append(r, Buffer{b, EdgeBuffer})
		}
	}
	for _, v := range p.Spec.Vertices {
		if v.Source != nil {
			r = append(r, Buffer{GenerateSourceBufferName(p.Namespace, p.Name, v.Name), SourceBuffer})
		} else if v.Sink != nil {
			r = append(r, Buffer{GenerateSinkBufferName(p.Namespace, p.Name, v.Name), SinkBuffer})
		}
	}
	return r
}

// GetDownstreamEdges returns all the downstream edges of a vertex
func (p Pipeline) GetDownstreamEdges(vertexName string) []Edge {
	var f func(vertexName string, edges *[]Edge)
	f = func(vertexName string, edges *[]Edge) {
		for _, b := range p.ListAllEdges() {
			if b.From == vertexName {
				*edges = append(*edges, b)
				f(b.To, edges)
			}
		}
	}
	result := []Edge{}
	f(vertexName, &result)
	return result
}

func (p Pipeline) GetDaemonServiceName() string {
	return fmt.Sprintf("%s-daemon-svc", p.Name)
}

func (p Pipeline) GetDaemonDeploymentName() string {
	return fmt.Sprintf("%s-daemon", p.Name)
}
func (p Pipeline) GetDaemonServiceURL() string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", p.GetDaemonServiceName(), p.Namespace, DaemonServicePort)
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
		{Name: "GODEBUG", Value: os.Getenv("GODEBUG")},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Ports:           []corev1.ContainerPort{{ContainerPort: DaemonServicePort}},
		Name:            CtrMain,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       standardResources,
		Env:             envVars,
		Args:            []string{"daemon-server", "--isbsvc-type=" + string(req.ISBSvcType)},
	}
	labels := map[string]string{
		KeyPartOf:       Project,
		KeyManagedBy:    ControllerPipeline,
		KeyComponent:    ComponentDaemon,
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
		{Name: "GODEBUG", Value: os.Getenv("GODEBUG")},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Name:            CtrInit,
		Env:             envVars,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       standardResources,
		Args:            []string{"isbsvc-buffer-validate", "--isbsvc-type=" + string(req.ISBSvcType)},
	}
	bfs := []string{}
	for _, b := range p.GetAllBuffers() {
		bfs = append(bfs, fmt.Sprintf("%s=%s", b.Name, b.Type))
	}
	c.Args = append(c.Args, "--buffers="+strings.Join(bfs, ","))
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
				{Port: DaemonServicePort, TargetPort: intstr.FromInt(int(DaemonServicePort))},
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
	defaultReadTimeout := time.Second
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
	// DeleteGracePeriodSeconds used to delete pipeline gracefully
	// +kubebuilder:default=30
	// +optional
	DeleteGracePeriodSeconds *int32 `json:"deleteGracePeriodSeconds,omitempty" protobuf:"varint,1,opt,name=deleteGracePeriodSeconds"`
	// DesiredPhase used to bring the pipeline from current phase to desired phase
	// +kubebuilder:default=Running
	// +optional
	DesiredPhase PipelinePhase `json:"desiredPhase,omitempty" protobuf:"bytes,2,opt,name=desiredPhase"`
}

// GetDeleteGracePeriodSeconds returns the value DeleteGracePeriodSeconds.
func (lc Lifecycle) GetDeleteGracePeriodSeconds() int32 {
	if lc.DeleteGracePeriodSeconds != nil {
		return *lc.DeleteGracePeriodSeconds
	}
	return 30
}

func (lc Lifecycle) GetDesiredPhase() PipelinePhase {
	if string(lc.DesiredPhase) != "" {
		return lc.DesiredPhase
	}
	return PipelinePhaseRunning
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
	// +kubebuilder:default={"deleteGracePeriodSeconds": 30, "desiredPhase": Running}
	// +optional
	Lifecycle Lifecycle `json:"lifecycle,omitempty" protobuf:"bytes,4,opt,name=lifecycle"`
	// Limits define the limitations such as buffer read batch size for all the vertices of a pipeline, they could be overridden by each vertex's settings
	// +kubebuilder:default={"readBatchSize": 500, "bufferMaxLength": 30000, "bufferUsageLimit": 80}
	// +optional
	Limits *PipelineLimits `json:"limits,omitempty" protobuf:"bytes,5,opt,name=limits"`
	// Watermark enables watermark progression across the entire pipeline. Updating this after the pipeline has been
	// created will have no impact and will be ignored. To make the pipeline honor any changes to the setting, the pipeline
	// should be recreated.
	// +kubebuilder:default={"disabled": false}
	// +optional
	Watermark Watermark `json:"watermark,omitempty" protobuf:"bytes,6,opt,name=watermark"`
	// Templates is used to customize additional kubernetes resources required for the Pipeline
	// +optional
	Templates *Templates `json:"templates,omitempty" protobuf:"bytes,7,opt,name=templates"`
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
}

// GetMaxDelay returns the configured max delay with a default value
func (wm Watermark) GetMaxDelay() time.Duration {
	if wm.MaxDelay != nil {
		return wm.MaxDelay.Duration
	}
	return time.Duration(0)
}

type Templates struct {
	// DaemonTemplate is used to customize the Daemon Deployment
	// +optional
	DaemonTemplate *DaemonTemplate `json:"daemon,omitempty" protobuf:"bytes,1,opt,name=daemon"`
	// JobTemplate is used to customize Jobs
	// +optional
	JobTemplate *JobTemplate `json:"job,omitempty" protobuf:"bytes,2,opt,name=job"`
}

type PipelineLimits struct {
	// Read batch size for all the vertices in the pipeline, can be overridden by the vertex's limit settings
	// +kubebuilder:default=500
	// +optional
	ReadBatchSize *uint64 `json:"readBatchSize,omitempty" protobuf:"varint,1,opt,name=readBatchSize"`
	// BufferMaxLength is used to define the max length of a buffer
	// Only applies to UDF and Source vertice as only they do buffer write.
	// It can be overridden by the settings in vertex limits.
	// +kubebuilder:default=50000
	// +optional
	BufferMaxLength *uint64 `json:"bufferMaxLength,omitempty" protobuf:"varint,2,opt,name=bufferMaxLength"`
	// BufferUsageLimit is used to define the percentage of the buffer usage limit, a valid value should be less than 100, for example, 85.
	// Only applies to UDF and Source vertice as only they do buffer write.
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
	Status      `json:",inline" protobuf:"bytes,1,opt,name=status"`
	Phase       PipelinePhase `json:"phase,omitempty" protobuf:"bytes,2,opt,name=phase,casttype=PipelinePhase"`
	Message     string        `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	LastUpdated metav1.Time   `json:"lastUpdated,omitempty" protobuf:"bytes,4,opt,name=lastUpdated"`
	VertexCount *uint32       `json:"vertexCount,omitempty" protobuf:"varint,5,opt,name=vertexCount"`
	SourceCount *uint32       `json:"sourceCount,omitempty" protobuf:"varint,6,opt,name=sourceCount"`
	SinkCount   *uint32       `json:"sinkCount,omitempty" protobuf:"varint,7,opt,name=sinkCount"`
	UDFCount    *uint32       `json:"udfCount,omitempty" protobuf:"varint,8,opt,name=udfCount"`
}

// SetVertexCounts sets the counts of vertices.
func (pls *PipelineStatus) SetVertexCounts(vertices []AbstractVertex) {
	var vertexCount = uint32(len(vertices))
	var sinkCount uint32
	var sourceCount uint32
	var udfCount uint32
	for _, v := range vertices {
		if v.Source != nil {
			sourceCount++
		}
		if v.Sink != nil {
			sinkCount++
		}
		if v.UDF != nil {
			udfCount++
		}
	}

	pls.VertexCount = &vertexCount
	pls.SinkCount = &sinkCount
	pls.SourceCount = &sourceCount
	pls.UDFCount = &udfCount
}

func (pls *PipelineStatus) SetPhase(phase PipelinePhase, msg string) {
	pls.Phase = phase
	pls.Message = msg
}

// InitConditions sets conditions to Unknown state.
func (pls *PipelineStatus) InitConditions() {
	pls.InitializeConditions(PipelineConditionConfigured, PipelineConditionDeployed)
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

// MarkPhaseRunning set the Pipeline has been running.
func (pls *PipelineStatus) MarkPhaseRunning() {
	pls.SetPhase(PipelinePhaseRunning, "")
}

// MarkDeployFailed set the Pipeline deployment failed
func (pls *PipelineStatus) MarkDeployFailed(reason, message string) {
	pls.MarkFalse(PipelineConditionDeployed, reason, message)
	pls.SetPhase(PipelinePhaseFailed, message)
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

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []Pipeline `json:"items" protobuf:"bytes,2,rep,name=items"`
}
