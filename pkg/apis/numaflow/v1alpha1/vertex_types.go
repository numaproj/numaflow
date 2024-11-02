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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/env"
	"k8s.io/utils/ptr"
)

// +kubebuilder:validation:Enum="";Running;Failed
type VertexPhase string

const (
	VertexPhaseUnknown VertexPhase = ""
	VertexPhaseRunning VertexPhase = "Running"
	VertexPhaseFailed  VertexPhase = "Failed"

	// VertexConditionDeployed has the status True when the vertex related sub resources are deployed.
	VertexConditionDeployed ConditionType = "Deployed"
	// VertexConditionPodsHealthy has the status True when all the vertex pods are healthy.
	VertexConditionPodsHealthy ConditionType = "PodsHealthy"
)

type VertexType string

const (
	VertexTypeSource    VertexType = "Source"
	VertexTypeSink      VertexType = "Sink"
	VertexTypeMapUDF    VertexType = "MapUDF"
	VertexTypeReduceUDF VertexType = "ReduceUDF"
)

const NumaflowRustBinary = "/bin/numaflow-rs"

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=vtx
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Desired",type=string,JSONPath=`.status.desiredReplicas`
// +kubebuilder:printcolumn:name="Current",type=string,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
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
	return v.Spec.IsASource()
}

func (v Vertex) HasUDTransformer() bool {
	return v.Spec.HasUDTransformer()
}

func (v Vertex) HasFallbackUDSink() bool {
	return v.Spec.HasFallbackUDSink()
}

func (v Vertex) IsUDSource() bool {
	return v.Spec.IsUDSource()
}

func (v Vertex) HasSideInputs() bool {
	return len(v.Spec.SideInputs) > 0
}

func (v Vertex) IsASink() bool {
	return v.Spec.IsASink()
}

func (v Vertex) IsUDSink() bool {
	return v.Spec.IsUDSink()
}

func (v Vertex) IsMapUDF() bool {
	return v.Spec.IsMapUDF()
}

func (v Vertex) IsReduceUDF() bool {
	return v.Spec.IsReduceUDF()
}

func (v Vertex) GetVertexType() VertexType {
	return v.Spec.GetVertexType()
}

func (v Vertex) Scalable() bool {
	if v.Spec.Scale.Disabled || v.IsReduceUDF() {
		return false
	}
	if v.IsASink() || v.IsMapUDF() || v.IsASource() {
		return true
	}
	return false
}

func (v Vertex) GetPartitionCount() int {
	return v.Spec.GetPartitionCount()
}

func (v Vertex) GetHeadlessServiceName() string {
	return v.Name + "-headless"
}

func (v Vertex) GetServiceObjs() []*corev1.Service {
	svcs := []*corev1.Service{v.getServiceObj(v.GetHeadlessServiceName(), true, VertexMetricsPort, VertexMetricsPortName)}
	if x := v.Spec.Source; x != nil && x.HTTP != nil && x.HTTP.Service {
		svcs = append(svcs, v.getServiceObj(v.Name, false, VertexHTTPSPort, VertexHTTPSPortName))
	}
	// serving source uses the same port as the http source, because both can't be configured at the same time
	if x := v.Spec.Source; x != nil && x.Serving != nil && x.Serving.Service {
		svcs = append(svcs, v.getServiceObj(v.Name, false, VertexHTTPSPort, VertexHTTPSPortName))
	}
	return svcs
}

func (v Vertex) GetServingSourceStreamName() string {
	return fmt.Sprintf("%s-%s-serving-source", v.Spec.PipelineName, v.Spec.Name)
}

func (v Vertex) getServiceObj(name string, headless bool, port int32, servicePortName string) *corev1.Service {
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
				{Port: port, TargetPort: intstr.FromInt32(port), Name: servicePortName},
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

// CommonEnvs returns the common envs for all vertex pod containers.
func (v Vertex) commonEnvs() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: EnvNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: EnvPod, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: EnvReplica, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + KeyReplica + "']"}}},
		{Name: EnvPipelineName, Value: v.Spec.PipelineName},
		{Name: EnvVertexName, Value: v.Spec.Name},
		{Name: EnvCallbackEnabled, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + CallbackEnabledKey + "']"}}},
		{Name: EnvCallbackURL, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + CallbackURLKey + "']"}}},
	}
}

// SidecarEnvs returns the envs for sidecar containers.
func (v Vertex) sidecarEnvs() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: EnvCPULimit, ValueFrom: &corev1.EnvVarSource{
			ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "limits.cpu"}}},
		{Name: EnvCPURequest, ValueFrom: &corev1.EnvVarSource{
			ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "requests.cpu"}}},
		{Name: EnvMemoryLimit, ValueFrom: &corev1.EnvVarSource{
			ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "limits.memory"}}},
		{Name: EnvMemoryRequest, ValueFrom: &corev1.EnvVarSource{
			ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "requests.memory"}}},
	}
}

func (v Vertex) GetPodSpec(req GetVertexPodSpecReq) (*corev1.PodSpec, error) {
	vertexCopy := &Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: v.Namespace,
			Name:      v.Name,
		},
		Spec: v.Spec.DeepCopyWithoutReplicas(),
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
	executeRustBinary, _ := env.GetBool(EnvExecuteRustBinary, false)
	containers, err := v.Spec.getType().getContainers(getContainerReq{
		isbSvcType:        req.ISBSvcType,
		env:               envVars,
		image:             req.Image,
		imagePullPolicy:   req.PullPolicy,
		resources:         req.DefaultResources,
		volumeMounts:      volumeMounts,
		executeRustBinary: executeRustBinary,
	})

	if err != nil {
		return nil, err
	}

	var readyzInitDeploy, readyzPeriodSeconds, readyzTimeoutSeconds, readyzFailureThreshold int32 = NumaContainerReadyzInitialDelaySeconds, NumaContainerReadyzPeriodSeconds, NumaContainerReadyzTimeoutSeconds, NumaContainerReadyzFailureThreshold
	var liveZInitDeploy, liveZPeriodSeconds, liveZTimeoutSeconds, liveZFailureThreshold int32 = NumaContainerLivezInitialDelaySeconds, NumaContainerLivezPeriodSeconds, NumaContainerLivezTimeoutSeconds, NumaContainerLivezFailureThreshold
	if x := v.Spec.ContainerTemplate; x != nil {
		readyzInitDeploy = GetProbeInitialDelaySecondsOr(x.ReadinessProbe, readyzInitDeploy)
		readyzPeriodSeconds = GetProbePeriodSecondsOr(x.ReadinessProbe, readyzPeriodSeconds)
		readyzTimeoutSeconds = GetProbeTimeoutSecondsOr(x.ReadinessProbe, readyzTimeoutSeconds)
		readyzFailureThreshold = GetProbeFailureThresholdOr(x.ReadinessProbe, readyzFailureThreshold)
		liveZInitDeploy = GetProbeInitialDelaySecondsOr(x.LivenessProbe, liveZInitDeploy)
		liveZPeriodSeconds = GetProbePeriodSecondsOr(x.LivenessProbe, liveZPeriodSeconds)
		liveZTimeoutSeconds = GetProbeTimeoutSecondsOr(x.LivenessProbe, liveZTimeoutSeconds)
		liveZFailureThreshold = GetProbeFailureThresholdOr(x.LivenessProbe, liveZFailureThreshold)
	}
	containers[0].ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/readyz",
				Port:   intstr.FromInt32(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: readyzInitDeploy,
		PeriodSeconds:       readyzPeriodSeconds,
		TimeoutSeconds:      readyzTimeoutSeconds,
		FailureThreshold:    readyzFailureThreshold,
	}

	containers[0].LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/livez",
				Port:   intstr.FromInt32(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: liveZInitDeploy,
		PeriodSeconds:       liveZPeriodSeconds,
		TimeoutSeconds:      liveZTimeoutSeconds,
		FailureThreshold:    liveZFailureThreshold,
	}
	containers[0].Ports = []corev1.ContainerPort{
		{Name: VertexMetricsPortName, ContainerPort: VertexMetricsPort},
	}

	if len(containers) > 1 { // udf, udsink, udsource, or source vertex specifies a udtransformer
		for i := 1; i < len(containers); i++ {
			containers[i].Env = append(containers[i].Env, v.commonEnvs()...)
			containers[i].Env = append(containers[i].Env, v.sidecarEnvs()...)
		}
	}

	initContainers := v.getInitContainers(req)

	if v.HasSideInputs() {
		sideInputsVolName := "var-run-side-inputs"
		volumes = append(volumes, corev1.Volume{
			Name:         sideInputsVolName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})

		sideInputsWatcher := corev1.Container{
			Name:            CtrSideInputsWatcher,
			Env:             req.Env,
			Image:           req.Image,
			ImagePullPolicy: req.PullPolicy,
			Resources:       req.DefaultResources,
			Args:            []string{"side-inputs-synchronizer", "--isbsvc-type=" + string(req.ISBSvcType), "--side-inputs-store=" + req.SideInputsStoreName, "--side-inputs=" + strings.Join(v.Spec.SideInputs, ",")},
		}
		sideInputsWatcher.Env = append(sideInputsWatcher.Env, v.commonEnvs()...)
		if x := v.Spec.SideInputsContainerTemplate; x != nil {
			x.ApplyToContainer(&sideInputsWatcher)
		}
		containers = append(containers, sideInputsWatcher)
		for i := 1; i < len(containers); i++ {
			if containers[i].Name == CtrSideInputsWatcher {
				containers[i].VolumeMounts = append(containers[i].VolumeMounts, corev1.VolumeMount{Name: sideInputsVolName, MountPath: PathSideInputsMount})
			} else {
				// Readonly mount for user-defined containers
				containers[i].VolumeMounts = append(containers[i].VolumeMounts, corev1.VolumeMount{Name: sideInputsVolName, MountPath: PathSideInputsMount, ReadOnly: true})
			}
		}
		// Side Inputs init container
		initContainers[1].VolumeMounts = append(initContainers[1].VolumeMounts, corev1.VolumeMount{Name: sideInputsVolName, MountPath: PathSideInputsMount})
	}

	if v.IsASource() && v.Spec.Source.Serving != nil {
		servingContainer, err := v.getServingContainer(req)
		if err != nil {
			return nil, err
		}
		containers = append(containers, servingContainer)
		// set the serving source stream name in the environment because the numa container will be reading from it
		containers[0].Env = append(containers[0].Env, corev1.EnvVar{Name: EnvServingJetstreamStream, Value: req.ServingSourceStreamName})
	}

	spec := &corev1.PodSpec{
		Subdomain:      v.GetHeadlessServiceName(),
		Volumes:        append(volumes, v.Spec.Volumes...),
		InitContainers: initContainers,
		Containers:     append(containers, v.Spec.Sidecars...),
	}
	v.Spec.AbstractPodTemplate.ApplyToPodSpec(spec)
	if v.Spec.ContainerTemplate != nil {
		v.Spec.ContainerTemplate.ApplyToNumaflowContainers(spec.Containers)
	}
	return spec, nil
}

func (v Vertex) getServingContainer(req GetVertexPodSpecReq) (corev1.Container, error) {
	servingSource := v.Spec.Source.Serving
	servingContainer := corev1.Container{
		Name:            ServingSourceContainer,
		Env:             req.Env,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Command:         []string{NumaflowRustBinary}, // we use the same image, but we execute the extension binary
		Resources:       req.DefaultResources,
		Args:            []string{"--serving"},
	}

	// set the common envs
	servingContainer.Env = append(servingContainer.Env, v.commonEnvs()...)

	// set the serving source stream name in the environment
	servingContainer.Env = append(servingContainer.Env, corev1.EnvVar{Name: EnvServingJetstreamStream, Value: req.ServingSourceStreamName})
	// set the serving source spec in the environment
	servingSourceCopy := servingSource.DeepCopy()
	servingSourceBytes, err := json.Marshal(servingSourceCopy)
	if err != nil {
		return corev1.Container{}, errors.New("failed to marshal serving source spec")
	}
	encodedServingSourceSpec := base64.StdEncoding.EncodeToString(servingSourceBytes)
	servingContainer.Env = append(servingContainer.Env, corev1.EnvVar{Name: EnvServingObject, Value: encodedServingSourceSpec})

	// set the serving source port in the environment
	servingContainer.Env = append(servingContainer.Env, corev1.EnvVar{Name: EnvServingPort, Value: strconv.Itoa(VertexHTTPSPort)})

	// Create a SimplifiedPipelineSpec and populate it with the vertex names and edges
	simplifiedPipelineSpec := PipelineSpec{
		Vertices: req.PipelineSpec.Vertices,
		Edges:    req.PipelineSpec.Edges,
	}

	pipelineSpecBytes, err := json.Marshal(simplifiedPipelineSpec)
	if err != nil {
		return corev1.Container{}, fmt.Errorf("failed to marshal pipeline spec, error: %w", err)
	}
	encodedPipelineSpec := base64.StdEncoding.EncodeToString(pipelineSpecBytes)
	servingContainer.Env = append(servingContainer.Env, corev1.EnvVar{
		Name:  EnvServingMinPipelineSpec,
		Value: encodedPipelineSpec,
	})

	// set the pod IP in the environment, which will be used for receiving callbacks.
	servingContainer.Env = append(servingContainer.Env, corev1.EnvVar{
		Name: EnvServingHostIP,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	})

	// set the serving store TTL in the environment
	servingContainer.Env = append(servingContainer.Env, corev1.EnvVar{
		Name:  EnvServingStoreTTL,
		Value: strconv.Itoa(int(servingSource.Store.GetTTL().Seconds())),
	})

	// if auth is configured, set the auth token in the environment
	if servingSource.Auth != nil && servingSource.Auth.Token != nil {
		servingContainer.Env = append(servingContainer.Env,
			corev1.EnvVar{
				Name: EnvServingAuthToken, ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: servingSource.Auth.Token.Name,
						},
						Key: servingSource.Auth.Token.Key,
					},
				},
			},
		)
	}

	servingContainer.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/readyz",
				Port:   intstr.FromInt32(VertexHTTPSPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 3,
		PeriodSeconds:       3,
		TimeoutSeconds:      1,
	}

	servingContainer.ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/livez",
				Port:   intstr.FromInt32(VertexHTTPSPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 20,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}

	return servingContainer, nil
}

func (v Vertex) getInitContainers(req GetVertexPodSpecReq) []corev1.Container {
	envVars := []corev1.EnvVar{
		{Name: EnvPipelineName, Value: v.Spec.PipelineName},
		{Name: EnvGoDebug, Value: os.Getenv(EnvGoDebug)},
	}
	envVars = append(envVars, req.Env...)
	initContainers := []corev1.Container{
		{
			Name:            CtrInit,
			Env:             envVars,
			Image:           req.Image,
			ImagePullPolicy: req.PullPolicy,
			Resources:       req.DefaultResources,
			Args:            []string{"isbsvc-validate", "--isbsvc-type=" + string(req.ISBSvcType)},
		},
	}
	if v.HasSideInputs() {
		initContainers = append(initContainers, corev1.Container{
			Name:            CtrInitSideInputs,
			Env:             envVars,
			Image:           req.Image,
			ImagePullPolicy: req.PullPolicy,
			Resources:       req.DefaultResources,
			Args:            []string{"side-inputs-init", "--isbsvc-type=" + string(req.ISBSvcType), "--side-inputs-store=" + req.SideInputsStoreName, "--side-inputs=" + strings.Join(v.Spec.SideInputs, ",")},
		})
	}

	if v.Spec.InitContainerTemplate != nil {
		v.Spec.InitContainerTemplate.ApplyToNumaflowContainers(initContainers)
	}
	return append(initContainers, v.Spec.InitContainers...)
}

func (vs VertexSpec) DeepCopyWithoutReplicas() VertexSpec {
	x := *vs.DeepCopy()
	x.Replicas = ptr.To[int32](0)
	return x
}

// OwnedBuffers returns the buffers that the vertex owns
func (v Vertex) OwnedBuffers() []string {
	return v.Spec.OwnedBufferNames(v.Namespace, v.Spec.PipelineName)
}

// GetFromBuckets returns the buckets that the vertex reads from.
// For a source vertex, it returns the source bucket name.
func (v Vertex) GetFromBuckets() []string {
	if v.IsASource() {
		return []string{GenerateSourceBucketName(v.Namespace, v.Spec.PipelineName, v.Spec.Name)}
	}
	r := []string{}
	for _, vt := range v.Spec.FromEdges {
		r = append(r, GenerateEdgeBucketName(v.Namespace, v.Spec.PipelineName, vt.From, vt.To))
	}
	return r
}

// GetToBuckets returns the buckets that the vertex writes to.
// For a sink vertex, it returns the sink bucket name.
func (v Vertex) GetToBuckets() []string {
	if v.IsASink() {
		return []string{GenerateSinkBucketName(v.Namespace, v.Spec.PipelineName, v.Spec.Name)}
	}
	r := []string{}
	for _, vt := range v.Spec.ToEdges {
		r = append(r, GenerateEdgeBucketName(v.Namespace, v.Spec.PipelineName, vt.From, vt.To))
	}
	return r
}

func (v Vertex) GetToBuffers() []string {
	r := []string{}
	if v.IsASink() {
		return r
	}
	for _, vt := range v.Spec.ToEdges {
		for i := 0; i < vt.GetToVertexPartitionCount(); i++ {
			r = append(r, GenerateBufferName(v.Namespace, v.Spec.PipelineName, vt.To, i))
		}
	}
	return r
}

func (v Vertex) GetReplicas() int {
	if v.IsReduceUDF() {
		// Replicas will be 0 only when pausing a pipeline
		if v.Spec.Replicas != nil && int(*v.Spec.Replicas) == 0 {
			return 0
		}
		// Replica of a reduce vertex is determined by the partitions.
		return v.GetPartitionCount()
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
	FromEdges []CombinedEdge `json:"fromEdges,omitempty" protobuf:"bytes,5,rep,name=fromEdges"`
	// +optional
	ToEdges []CombinedEdge `json:"toEdges,omitempty" protobuf:"bytes,6,rep,name=toEdges"`
	// Watermark indicates watermark progression in the vertex, it's populated from the pipeline watermark settings.
	// +kubebuilder:default={"disabled": false}
	// +optional
	Watermark Watermark `json:"watermark,omitempty" protobuf:"bytes,7,opt,name=watermark"`
}

type AbstractVertex struct {
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// +optional
	Source *Source `json:"source,omitempty" protobuf:"bytes,2,opt,name=source"`
	// +optional
	Sink *Sink `json:"sink,omitempty" protobuf:"bytes,3,opt,name=sink"`
	// +optional
	UDF *UDF `json:"udf,omitempty" protobuf:"bytes,4,opt,name=udf"`
	// Container template for the main numa container.
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,5,opt,name=containerTemplate"`
	// Container template for all the vertex pod init containers spawned by numaflow, excluding the ones specified by the user.
	// +optional
	InitContainerTemplate *ContainerTemplate `json:"initContainerTemplate,omitempty" protobuf:"bytes,6,opt,name=initContainerTemplate"`
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,7,opt,name=abstractPodTemplate"`
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=name
	Volumes []corev1.Volume `json:"volumes,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,8,rep,name=volumes"`
	// Limits define the limitations such as buffer read batch size for all the vertices of a pipeline, will override pipeline level settings
	// +optional
	Limits *VertexLimits `json:"limits,omitempty" protobuf:"bytes,9,opt,name=limits"`
	// Settings for autoscaling
	// +optional
	Scale Scale `json:"scale,omitempty" protobuf:"bytes,10,opt,name=scale"`
	// List of customized init containers belonging to the pod.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
	// +optional
	InitContainers []corev1.Container `json:"initContainers,omitempty" protobuf:"bytes,11,rep,name=initContainers"`
	// List of customized sidecar containers belonging to the pod.
	// +optional
	Sidecars []corev1.Container `json:"sidecars,omitempty" protobuf:"bytes,12,rep,name=sidecars"`
	// Number of partitions of the vertex owned buffers.
	// It applies to udf and sink vertices only.
	// +optional
	Partitions *int32 `json:"partitions,omitempty" protobuf:"bytes,13,opt,name=partitions"`
	// Names of the side inputs used in this vertex.
	// +optional
	SideInputs []string `json:"sideInputs,omitempty" protobuf:"bytes,14,rep,name=sideInputs"`
	// Container template for the side inputs watcher container.
	// +optional
	SideInputsContainerTemplate *ContainerTemplate `json:"sideInputsContainerTemplate,omitempty" protobuf:"bytes,15,opt,name=sideInputsContainerTemplate"`
	// The strategy to use to replace existing pods with new ones.
	// +kubebuilder:default={"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": "25%"}}
	// +optional
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,16,opt,name=updateStrategy"`
}

func (av AbstractVertex) GetVertexType() VertexType {
	if av.IsASource() {
		return VertexTypeSource
	} else if av.IsASink() {
		return VertexTypeSink
	} else if av.IsMapUDF() {
		return VertexTypeMapUDF
	} else if av.IsReduceUDF() {
		return VertexTypeReduceUDF
	}
	// This won't happen
	return ""
}

func (av AbstractVertex) GetPartitionCount() int {
	if av.Partitions == nil || *av.Partitions < 1 {
		return 1
	}
	if av.IsASource() || (av.IsReduceUDF() && !av.UDF.GroupBy.Keyed) {
		return 1
	}
	return int(*av.Partitions)
}

func (av AbstractVertex) IsASource() bool {
	return av.Source != nil
}

func (av AbstractVertex) HasUDTransformer() bool {
	return av.Source != nil && av.Source.UDTransformer != nil
}

func (av AbstractVertex) HasFallbackUDSink() bool {
	return av.IsASink() && av.Sink.Fallback != nil && av.Sink.Fallback.UDSink != nil
}

func (av AbstractVertex) IsUDSource() bool {
	return av.IsASource() && av.Source.UDSource != nil
}

func (av AbstractVertex) IsASink() bool {
	return av.Sink != nil
}

func (av AbstractVertex) IsUDSink() bool {
	return av.IsASink() && av.Sink.UDSink != nil
}

func (av AbstractVertex) IsMapUDF() bool {
	return av.UDF != nil && av.UDF.GroupBy == nil
}

func (av AbstractVertex) IsReduceUDF() bool {
	return av.UDF != nil && av.UDF.GroupBy != nil
}

func (av AbstractVertex) OwnedBufferNames(namespace, pipeline string) []string {
	var r []string
	if av.IsASource() {
		return r
	}
	for i := 0; i < av.GetPartitionCount(); i++ {
		r = append(r, GenerateBufferName(namespace, pipeline, av.Name, i))
	}
	return r
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
	// BufferMaxLength is used to define the max length of a buffer.
	// It overrides the settings from pipeline limits.
	// +optional
	BufferMaxLength *uint64 `json:"bufferMaxLength,omitempty" protobuf:"varint,3,opt,name=bufferMaxLength"`
	// BufferUsageLimit is used to define the percentage of the buffer usage limit, a valid value should be less than 100, for example, 85.
	// It overrides the settings from pipeline limits.
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
	Status `json:",inline" protobuf:"bytes,1,opt,name=status"`
	// +optional
	Phase VertexPhase `json:"phase" protobuf:"bytes,2,opt,name=phase,casttype=VertexPhase"`
	// Total number of non-terminated pods targeted by this Vertex (their labels match the selector).
	// +optional
	Replicas uint32 `json:"replicas" protobuf:"varint,3,opt,name=replicas"`
	// The number of desired replicas.
	// +optional
	DesiredReplicas uint32 `json:"desiredReplicas" protobuf:"varint,4,opt,name=desiredReplicas"`
	// +optional
	Selector string `json:"selector,omitempty" protobuf:"bytes,5,opt,name=selector"`
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,6,opt,name=reason"`
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,7,opt,name=message"`
	// Time of last scaling operation.
	// +optional
	LastScaledAt metav1.Time `json:"lastScaledAt,omitempty" protobuf:"bytes,8,opt,name=lastScaledAt"`
	// The generation observed by the Vertex controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,9,opt,name=observedGeneration"`
	// The number of pods targeted by this Vertex with a Ready Condition.
	// +optional
	ReadyReplicas uint32 `json:"readyReplicas,omitempty" protobuf:"varint,10,opt,name=readyReplicas"`
	// The number of Pods created by the controller from the Vertex version indicated by updateHash.
	UpdatedReplicas uint32 `json:"updatedReplicas,omitempty" protobuf:"varint,11,opt,name=updatedReplicas"`
	// The number of ready Pods created by the controller from the Vertex version indicated by updateHash.
	UpdatedReadyReplicas uint32 `json:"updatedReadyReplicas,omitempty" protobuf:"varint,12,opt,name=updatedReadyReplicas"`
	// If not empty, indicates the current version of the Vertex used to generate Pods.
	CurrentHash string `json:"currentHash,omitempty" protobuf:"bytes,13,opt,name=currentHash"`
	// If not empty, indicates the updated version of the Vertex used to generate Pods.
	UpdateHash string `json:"updateHash,omitempty" protobuf:"bytes,14,opt,name=updateHash"`
}

func (vs *VertexStatus) MarkPhase(phase VertexPhase, reason, message string) {
	vs.Phase = phase
	vs.Reason = reason
	vs.Message = message
}

// MarkPhaseFailed marks the phase as failed with the given reason and message.
func (vs *VertexStatus) MarkPhaseFailed(reason, message string) {
	vs.MarkPhase(VertexPhaseFailed, reason, message)
}

// MarkPhaseRunning marks the phase as running.
func (vs *VertexStatus) MarkPhaseRunning() {
	vs.MarkPhase(VertexPhaseRunning, "", "")
}

// MarkDeployed set the Vertex has it's sub resources deployed.
func (vs *VertexStatus) MarkDeployed() {
	vs.MarkTrue(VertexConditionDeployed)
}

// MarkDeployFailed set the Vertex deployment failed
func (vs *VertexStatus) MarkDeployFailed(reason, message string) {
	vs.MarkFalse(VertexConditionDeployed, reason, message)
	vs.MarkPhaseFailed(reason, message)
}

// MarkPodNotHealthy marks the pod not healthy with the given reason and message.
func (vs *VertexStatus) MarkPodNotHealthy(reason, message string) {
	vs.MarkFalse(VertexConditionPodsHealthy, reason, message)
	vs.Reason = reason
	vs.Message = "Degraded: " + message
}

// MarkPodHealthy marks the pod as healthy with the given reason and message.
func (vs *VertexStatus) MarkPodHealthy(reason, message string) {
	vs.MarkTrueWithReason(VertexConditionPodsHealthy, reason, message)
}

// InitConditions sets conditions to Unknown state.
func (vs *VertexStatus) InitConditions() {
	vs.InitializeConditions(VertexConditionDeployed, VertexConditionPodsHealthy)
}

// IsHealthy indicates whether the vertex is healthy or not
func (vs *VertexStatus) IsHealthy() bool {
	if vs.Phase != VertexPhaseRunning {
		return false
	}
	return vs.IsReady()
}

// SetObservedGeneration sets the Status ObservedGeneration
func (vs *VertexStatus) SetObservedGeneration(value int64) {
	vs.ObservedGeneration = value
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type VertexList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []Vertex `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func GenerateBufferName(namespace, pipelineName, vertex string, index int) string {
	return fmt.Sprintf("%s-%s-%s-%d", namespace, pipelineName, vertex, index)
}

func GenerateBufferNames(namespace, pipelineName, vertex string, numOfPartitions int) []string {
	var result []string
	for i := 0; i < numOfPartitions; i++ {
		result = append(result, GenerateBufferName(namespace, pipelineName, vertex, i))
	}
	return result
}

func GenerateSourceBucketName(namespace, pipeline, vertex string) string {
	return fmt.Sprintf("%s-%s-%s_SOURCE", namespace, pipeline, vertex)
}

func GenerateSinkBucketName(namespace, pipelineName, vertex string) string {
	return fmt.Sprintf("%s-%s-%s_SINK", namespace, pipelineName, vertex)
}

type VertexTemplate struct {
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,1,opt,name=abstractPodTemplate"`
	// Template for the vertex numa container
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,2,opt,name=containerTemplate"`
	// Template for the vertex init container
	// +optional
	InitContainerTemplate *ContainerTemplate `json:"initContainerTemplate,omitempty" protobuf:"bytes,3,opt,name=initContainerTemplate"`
}
