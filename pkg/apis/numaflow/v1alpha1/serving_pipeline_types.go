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
	"strconv"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
)

// +kubebuilder:validation:Enum="";Running;Failed;Deleting
type ServingPipelinePhase string

const (
	ServingPipelinePhaseUnknown  ServingPipelinePhase = ""
	ServingPipelinePhaseRunning  ServingPipelinePhase = "Running"
	ServingPipelinePhaseFailed   ServingPipelinePhase = "Failed"
	ServingPipelinePhaseDeleting ServingPipelinePhase = "Deleting"

	// ServingPipelineConditionConfigured has the status True when the ServingPipeline
	// has valid configuration.
	ServingPipelineConditionConfigured ConditionType = "Configured"
	// ServingPipelineConditionDeployed has the status True when the ServingPipeline
	// has its orchestrated children created.
	ServingPipelineConditionDeployed ConditionType = "Deployed"
)

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=spl
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
type ServingPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec ServingPipelineSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// +optional
	Status ServingPipelineStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

type ServingPipelineSpec struct {
	Serving  ServingSpec  `json:"serving,omitempty" protobuf:"bytes,1,opt,name=serving"`
	Pipeline PipelineSpec `json:"pipeline,omitempty" protobuf:"bytes,2,opt,name=pipeline"`
}

type ServingSpec struct {
	// +optional
	Auth *Authorization `json:"auth" protobuf:"bytes,1,opt,name=auth"`
	// Whether to create a ClusterIP Service
	// +optional
	Service bool `json:"service" protobuf:"bytes,2,opt,name=service"`
	// The header key from which the message id will be extracted
	MsgIDHeaderKey *string `json:"msgIDHeaderKey" protobuf:"bytes,3,opt,name=msgIDHeaderKey"`
	// Request timeout in seconds. Default value is 120 seconds.
	// +optional
	RequestTimeoutSecs *uint32 `json:"requestTimeoutSeconds,omitempty" protobuf:"varint,4,opt,name=requestTimeoutSeconds"`
	// +optional
	ServingStore *ServingStore `json:"store,omitempty" protobuf:"bytes,5,rep,name=store"`
}

// ServingStore defines information of a Serving Store used in a pipeline
type ServingStore struct {
	Container *Container `json:"container" protobuf:"bytes,1,opt,name=container"`
}

type ServingPipelineStatus struct {
	Status `json:",inline" protobuf:"bytes,1,opt,name=status"`
	// +optional
	Phase ServingPipelinePhase `json:"phase,omitempty" protobuf:"bytes,2,opt,name=phase,casttype=PipelinePhase"`
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// +optional
	LastUpdated metav1.Time `json:"lastUpdated,omitempty" protobuf:"bytes,4,opt,name=lastUpdated"`
	// The generation observed by the ServingPipeline controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,11,opt,name=observedGeneration"`
}

// Generate the stream name in JetStream used for serving source
func (sp ServingPipeline) GenerateSourceStreamName() string {
	return fmt.Sprintf("serving-source-%s", sp.Name)
}

func (sp ServingPipeline) GetServingStoreName() string {
	return fmt.Sprintf("serving-store-%s", sp.Name)
}

func (sp ServingPipeline) GetPipelineName() string {
	return fmt.Sprintf("s-%s", sp.Name)
}

func (sp ServingPipeline) GetServingServerName() string {
	return fmt.Sprintf("%s-serving", sp.Name)
}

func (sp ServingPipeline) GetServingServiceName() string {
	return fmt.Sprintf("%s-serving", sp.Name)
}

func (sp ServingPipeline) GetServingServiceObj() *corev1.Service {
	if !sp.Spec.Serving.Service {
		return nil
	}
	labels := map[string]string{
		KeyPartOf:              Project,
		KeyManagedBy:           ControllerServingPipeline,
		KeyComponent:           ComponentServingServer,
		KeyServingPipelineName: sp.Name,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: sp.Namespace,
			Name:      sp.GetServingServiceName(),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(sp.GetObjectMeta(), ServingPipelineGroupVersionKind),
			},
			Labels: labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "tcp", Port: ServingServicePort, TargetPort: intstr.FromInt32(ServingServicePort)},
			},
			Selector: labels,
		},
	}
}

func (sp ServingPipeline) GetServingDeploymentObj(req GetServingPipelineResourceReq) (*appv1.Deployment, error) {
	pl := sp.GetPipelineObj(req)
	simplifiedPipelineSpec := PipelineSpec{
		Vertices: pl.Spec.Vertices,
		Edges:    pl.Spec.Edges,
	}

	pipelineSpecBytes, err := json.Marshal(simplifiedPipelineSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pipeline spec, error: %w", err)
	}
	encodedPipelineSpec := base64.StdEncoding.EncodeToString(pipelineSpecBytes)

	servingSourceSettings, err := json.Marshal(sp.Spec.Serving)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal serving source settings: %w", err)
	}
	encodedServingSourceSettings := base64.StdEncoding.EncodeToString(servingSourceSettings)

	envVars := []corev1.EnvVar{
		{Name: EnvNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: EnvPod, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: EnvServingMinPipelineSpec, Value: encodedPipelineSpec},
		{Name: "NUMAFLOW_SERVING_SOURCE_SETTINGS", Value: encodedServingSourceSettings},
		{Name: "NUMAFLOW_SERVING_KV_STORE", Value: fmt.Sprintf("%s_SERVING_KV_STORE", sp.GetServingStoreName())},
		{Name: EnvServingPort, Value: strconv.Itoa(ServingServicePort)},
		{ // TODO: do we still need it?
			Name: EnvServingHostIP,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Ports:           []corev1.ContainerPort{{ContainerPort: ServingServicePort}},
		Name:            CtrMain,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       req.DefaultResources, // TODO: need to have a way to override.
		Env:             envVars,
		Command:         []string{NumaflowRustBinary},
		Args:            []string{"--serving"},
	}
	labels := map[string]string{
		KeyPartOf:              Project,
		KeyManagedBy:           ControllerServingPipeline,
		KeyComponent:           ComponentServingServer,
		KeyAppName:             sp.GetServingServerName(),
		KeyServingPipelineName: sp.Name,
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
				InitContainers: []corev1.Container{sp.getStreamValidationInitContainerSpec(req)},
			},
		},
	}
	// TODO: (k8s 1.29)  clean this up once we deprecate the support for k8s < 1.29
	if isSidecarSupported() {
		spec.Template.Spec.InitContainers = append(spec.Template.Spec.InitContainers, sp.getStoreSidecarContainerSpec(req)...)
	} else {
		spec.Template.Spec.Containers = append(spec.Template.Spec.Containers, sp.getStoreSidecarContainerSpec(req)...)
	}
	// TODO(spl): add template
	return &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: sp.Namespace,
			Name:      sp.GetServingServerName(),
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(sp.GetObjectMeta(), ServingPipelineGroupVersionKind),
			},
		},
		Spec: spec,
	}, nil
}

func (sp ServingPipeline) GetPipelineObj(req GetServingPipelineResourceReq) Pipeline {
	servingSourceSettings, _ := json.Marshal(sp.Spec.Serving)
	encodedServingSourceSettings := base64.StdEncoding.EncodeToString(servingSourceSettings)
	// The pipeline spec should have been validated
	plSpec := sp.Spec.Pipeline.DeepCopy()
	for i := range plSpec.Vertices {
		if plSpec.Vertices[i].IsASource() { // Must be serving source, replace it
			jsSrc := sp.buildJetStreamSource(req)
			plSpec.Vertices[i].Source = &Source{
				JetStream: &jsSrc,
			}
			// Validation container
			plSpec.Vertices[i].InitContainers = append(plSpec.Vertices[i].InitContainers, sp.getStreamValidationInitContainerSpec(req))
		}
		if plSpec.Vertices[i].IsASink() {
			// TODO: (k8s 1.29)  clean this up once we deprecate the support for k8s < 1.29
			if isSidecarSupported() {
				plSpec.Vertices[i].InitContainers = append(plSpec.Vertices[i].InitContainers, sp.getStoreSidecarContainerSpec(req)...)
			} else {
				plSpec.Vertices[i].Sidecars = append(plSpec.Vertices[i].Sidecars, sp.getStoreSidecarContainerSpec(req)...)
			}
		}
		if plSpec.Vertices[i].ContainerTemplate == nil {
			plSpec.Vertices[i].ContainerTemplate = &ContainerTemplate{}
		}
		plSpec.Vertices[i].ContainerTemplate.Env = append(
			plSpec.Vertices[i].ContainerTemplate.Env,
			corev1.EnvVar{Name: EnvCallbackEnabled, Value: "true"},
			corev1.EnvVar{Name: "NUMAFLOW_SERVING_SOURCE_SETTINGS", Value: encodedServingSourceSettings},
			corev1.EnvVar{Name: "NUMAFLOW_SERVING_KV_STORE", Value: fmt.Sprintf("%s_SERVING_KV_STORE", sp.GetServingStoreName())},
		)
	}
	labels := map[string]string{
		KeyPartOf:              Project,
		KeyManagedBy:           ControllerServingPipeline,
		KeyComponent:           ComponentPipeline,
		KeyServingPipelineName: sp.Name,
	}
	pl := Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: sp.Namespace,
			Name:      sp.GetPipelineName(),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(sp.GetObjectMeta(), ServingPipelineGroupVersionKind),
			},
			Labels: labels,
		},
		Spec: *plSpec,
	}
	return pl
}

func (sp ServingPipeline) buildJetStreamSource(req GetServingPipelineResourceReq) JetStreamSource {
	// The isbsvc should have been validated
	jsSrc := JetStreamSource{
		URL:    req.ISBSvcConfig.JetStream.URL,
		Stream: sp.GenerateSourceStreamName(),
	}
	if x := req.ISBSvcConfig.JetStream; x != nil {
		if x.TLSEnabled {
			jsSrc.TLS = &TLS{
				InsecureSkipVerify: true,
			}
		}
		if x.Auth != nil && x.Auth.Basic != nil && x.Auth.Basic.User != nil && x.Auth.Basic.Password != nil {
			jsSrc.Auth = &NatsAuth{
				Basic: &BasicAuth{
					User: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: x.Auth.Basic.User.Name,
						},
						Key: x.Auth.Basic.User.Key,
					},
					Password: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: x.Auth.Basic.Password.Name,
						},
						Key: x.Auth.Basic.Password.Key,
					},
				},
			}
		}
	}
	return jsSrc
}

func (sp ServingPipeline) getStreamValidationInitContainerSpec(req GetServingPipelineResourceReq) corev1.Container {
	envVars := []corev1.EnvVar{
		{Name: EnvPipelineName, Value: sp.GetPipelineName()},
		{Name: EnvGoDebug, Value: os.Getenv(EnvGoDebug)},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Name:            "validate-stream-init",
		Env:             envVars,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       req.DefaultResources,
		Args:            []string{"isbsvc-validate", "--isbsvc-type=" + string(ISBSvcTypeJetStream), "--buffers=" + sp.GenerateSourceStreamName()},
	}
	return c
}

func (sp ServingPipeline) getStoreSidecarContainerSpec(req GetServingPipelineResourceReq) []corev1.Container {
	if x := sp.Spec.Serving.ServingStore; x != nil && x.Container != nil {
		cb := containerBuilder{}.
			name(CtrUdStore).
			image(x.Container.Image).
			imagePullPolicy(req.PullPolicy). // Default pull policy
			appendEnv(x.Container.Env...).
			appendVolumeMounts(x.Container.VolumeMounts...).
			resources(x.Container.Resources).
			securityContext(x.Container.SecurityContext).
			appendEnvFrom(x.Container.EnvFrom...).
			appendPorts(x.Container.Ports...).
			asSidecar()

		if len(x.Container.Command) > 0 {
			cb = cb.command(x.Container.Command...)
		}
		if len(x.Container.Args) > 0 {
			cb = cb.args(x.Container.Args...)
		}
		if x.Container.ImagePullPolicy != nil {
			cb = cb.imagePullPolicy(*x.Container.ImagePullPolicy)
		}
		container := cb.build()
		container.LivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/sidecar-livez",
					Port:   intstr.FromInt32(VertexMetricsPort),
					Scheme: corev1.URISchemeHTTPS,
				},
			},
			InitialDelaySeconds: GetProbeInitialDelaySecondsOr(x.Container.LivenessProbe, UDContainerLivezInitialDelaySeconds),
			PeriodSeconds:       GetProbePeriodSecondsOr(x.Container.LivenessProbe, UDContainerLivezPeriodSeconds),
			TimeoutSeconds:      GetProbeTimeoutSecondsOr(x.Container.LivenessProbe, UDContainerLivezTimeoutSeconds),
			FailureThreshold:    GetProbeFailureThresholdOr(x.Container.LivenessProbe, UDContainerLivezFailureThreshold),
		}
		return []corev1.Container{container}
	}
	return nil
}

func (spls *ServingPipelineStatus) SetPhase(phase ServingPipelinePhase, msg string) {
	spls.Phase = phase
	spls.Message = msg
}

// InitConditions sets conditions to Unknown state.
func (spls *ServingPipelineStatus) InitConditions() {
	spls.InitializeConditions(ServingPipelineConditionConfigured, ServingPipelineConditionDeployed)
}

// MarkConfigured set the ServingPipeline has valid configuration.
func (spls *ServingPipelineStatus) MarkConfigured() {
	spls.MarkTrue(ServingPipelineConditionConfigured)
}

// MarkNotConfigured the ServingPipeline has configuration.
func (spls *ServingPipelineStatus) MarkNotConfigured(reason, message string) {
	spls.MarkFalse(ServingPipelineConditionConfigured, reason, message)
	spls.SetPhase(ServingPipelinePhaseFailed, message)
}

// MarkDeployed set the ServingPipeline has been deployed.
func (spls *ServingPipelineStatus) MarkDeployed() {
	spls.MarkTrue(ServingPipelineConditionDeployed)
}

// MarkDeployFailed set the ServingPipeline deployment failed
func (spls *ServingPipelineStatus) MarkDeployFailed(reason, message string) {
	spls.MarkFalse(ServingPipelineConditionDeployed, reason, message)
	spls.SetPhase(ServingPipelinePhaseFailed, message)
}

// MarkPhaseRunning set the ServingPipeline has been running.
func (spls *ServingPipelineStatus) MarkPhaseRunning() {
	spls.SetPhase(ServingPipelinePhaseRunning, "")
}

// MarkPhaseDeleting set the ServingPipeline is deleting.
func (spls *ServingPipelineStatus) MarkPhaseDeleting() {
	spls.SetPhase(ServingPipelinePhaseDeleting, "Deleting in progress")
}

// SetObservedGeneration sets the Status ObservedGeneration
func (spls *ServingPipelineStatus) SetObservedGeneration(value int64) {
	spls.ObservedGeneration = value
}

// IsHealthy indicates whether the ServingPipeline is in healthy status
func (pls *ServingPipelineStatus) IsHealthy() bool {
	switch pls.Phase {
	case ServingPipelinePhaseFailed:
		return false
	case ServingPipelinePhaseRunning:
		return pls.IsReady()
	case ServingPipelinePhaseDeleting:
		// Transient phases, return true
		return true
	default:
		return false
	}
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ServingPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []Pipeline `json:"items" protobuf:"bytes,2,rep,name=items"`
}
