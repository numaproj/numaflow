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

// FindEdgeWithBuffer is used to locate the edge of the buffer.
func (p Pipeline) FindEdgeWithBuffer(buffer string) *Edge {
	for _, e := range p.Spec.Edges {
		if buffer == GenerateEdgeBufferName(p.Namespace, p.Name, e.From, e.To) {
			return &e
		}
	}
	return nil
}

func (p Pipeline) GetToEdges(vertexName string) []Edge {
	edges := []Edge{}
	for _, e := range p.Spec.Edges {
		if e.From == vertexName {
			edges = append(edges, e)
		}
	}
	return edges
}

func (p Pipeline) GetFromEdges(vertexName string) []Edge {
	edges := []Edge{}
	for _, e := range p.Spec.Edges {
		if e.To == vertexName {
			edges = append(edges, e)
		}
	}
	return edges
}

func (p Pipeline) GetAllBuffers() []Buffer {
	r := []Buffer{}
	for _, e := range p.Spec.Edges {
		r = append(r, Buffer{GenerateEdgeBufferName(p.Namespace, p.Name, e.From, e.To), EdgeBuffer})
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
		Resources:       standardResources, // How to customize resources?
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
		Replicas: pointer.Int32(1),
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
			},
			Spec: corev1.PodSpec{
				Containers:     []corev1.Container{c},
				InitContainers: []corev1.Container{p.getDaemonPodInitContainer(req)},
			},
		},
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

type Lifecycle struct {
	// DeleteGracePeriodSeconds used to delete pipeline gracefully
	// +kubebuilder:default=30
	// +optional
	DeleteGracePeriodSeconds int32 `json:"deleteGracePeriodSeconds,omitempty" protobuf:"varint,1,opt,name=deleteGracePeriodSeconds"`

	// DesiredPhase used to bring the pipeline from current phase to desired phase
	// +kubebuilder:default=Running
	// +optional
	DesiredPhase PipelinePhase `json:"desiredPhase,omitempty" protobuf:"bytes,2,opt,name=desiredPhase"`
}

type PipelineSpec struct {
	// +optional
	InterStepBufferServiceName string `json:"interStepBufferServiceName,omitempty" protobuf:"bytes,1,opt,name=interStepBufferServiceName"`
	// +patchStrategy=merge
	// +patchMergeKey=name
	Vertices []AbstractVertex `json:"vertices,omitempty" protobuf:"bytes,2,rep,name=vertices"`
	// Edges define the relationships between vertices
	Edges []Edge `json:"edges,omitempty" protobuf:"bytes,3,rep,name=edges"`
	// Lifecycle define the Lifecycle properties
	// +patchStrategy=merge
	// +patchMergeKey=name
	// +kubebuilder:default={"deleteGracePeriodSeconds": 30, "desiredPhase": Running}
	// +optional
	Lifecycle Lifecycle `json:"lifecycle,omitempty" protobuf:"bytes,4,opt,name=lifecycle"`
	// Limits define the limitations such as buffer read batch size for all the vertices of a pipleine, they could be overridden by each vertex's settings
	// +kubebuilder:default={"readBatchSize": 500, "bufferMaxLength": 30000, "bufferUsageLimit": 80}
	// +optional
	Limits *PipelineLimits `json:"limits,omitempty" protobuf:"bytes,5,opt,name=limits"`
	// Watermark enables watermark progression across the entire pipeline. Updating this after the pipeline has been
	// created will have no impact and will be ignored. To make the pipeline honor any changes to the setting, the pipeline
	// should be recreated.
	// +kubebuilder:default={"propagate": false}
	// +optional
	Watermark Watermark `json:"watermark,omitempty" protobuf:"bytes,6,opt,name=watermark"`
}

type Watermark struct {
	// Propagate toggles the watermark propagation.
	// +kubebuilder:default=false
	// +optional
	Propagate bool `json:"propagate,omitempty" protobuf:"bytes,1,opt,name=propagate"`
}

type PipelineLimits struct {
	// Read batch size for all the vertices in the pipeline, can be overridden by the vertex's limit settings
	// +kubebuilder:default=500
	// +optional
	ReadBatchSize *uint64 `json:"readBatchSize,omitempty" protobuf:"varint,1,opt,name=readBatchSize"`
	// BufferMaxLength is used to define the max length of a buffer
	// Only applies to UDF and Source vertice as only they do buffer write.
	// It can be overridden by the settings in vertex limits.
	// +kubebuilder:default=30000
	// +optional
	BufferMaxLength *uint64 `json:"bufferMaxLength,omitempty" protobuf:"varint,2,opt,name=bufferMaxLength"`
	// BufferUsageLimit is used to define the pencentage of the buffer usage limit, a valid value should be less than 100, for example, 85.
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
