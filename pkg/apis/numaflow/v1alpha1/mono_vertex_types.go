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
	"sort"
	"strconv"
	"time"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// +kubebuilder:validation:Enum="";Running;Failed;Pausing;Paused;Deleting
type MonoVertexPhase string

const (
	MonoVertexPhaseUnknown MonoVertexPhase = ""
	MonoVertexPhaseRunning MonoVertexPhase = "Running"
	MonoVertexPhaseFailed  MonoVertexPhase = "Failed"
	MonoVertexPhasePaused  MonoVertexPhase = "Paused"

	// MonoVertexConditionDeployed has the status True when the MonoVertex
	// has its sub resources created and deployed.
	MonoVertexConditionDeployed ConditionType = "Deployed"
	// MonoVertexConditionDaemonHealthy has the status True when the daemon service of the mono vertex is healthy.
	MonoVertexConditionDaemonHealthy ConditionType = "DaemonHealthy"
	// MonoVertexPodsHealthy has the status True when the pods of the mono vertex are healthy
	MonoVertexPodsHealthy ConditionType = "PodsHealthy"
)

func (mvp MonoVertexPhase) Code() int {
	switch mvp {
	case MonoVertexPhaseUnknown:
		return 0
	case MonoVertexPhaseRunning:
		return 1
	case MonoVertexPhasePaused:
		return 2
	case MonoVertexPhaseFailed:
		return 3
	default:
		return 0
	}
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=mvtx
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
type MonoVertex struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec MonoVertexSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	// +optional
	Status MonoVertexStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

func (mv MonoVertex) getReplicas() int {
	if mv.Spec.Replicas == nil {
		return 1
	}
	return int(*mv.Spec.Replicas)
}

func (mv MonoVertex) CalculateReplicas() int {
	// If we are pausing the MonoVertex then we should have the desired replicas as 0
	if mv.Spec.Lifecycle.GetDesiredPhase() == MonoVertexPhasePaused {
		return 0
	}
	desiredReplicas := mv.getReplicas()
	// Don't allow replicas to be out of the range of min and max when auto scaling is enabled
	if s := mv.Spec.Scale; !s.Disabled {
		max := int(s.GetMaxReplicas())
		min := int(s.GetMinReplicas())
		if desiredReplicas < min {
			desiredReplicas = min
		} else if desiredReplicas > max {
			desiredReplicas = max
		}
	}
	return desiredReplicas
}

func (mv MonoVertex) GetHeadlessServiceName() string {
	return mv.Name + "-mv-headless"
}

func (mv MonoVertex) GetServiceObjs() []*corev1.Service {
	ports := map[string]int32{
		MonoVertexMetricsPortName: MonoVertexMetricsPort,
		MonoVertexMonitorPortName: MonoVertexMonitorPort,
	}
	svcs := []*corev1.Service{mv.getServiceObj(mv.GetHeadlessServiceName(), true, ports)}
	if x := mv.Spec.Source; x != nil && x.HTTP != nil && x.HTTP.Service {
		svcs = append(svcs, mv.getServiceObj(mv.Name, false, map[string]int32{VertexHTTPSPortName: VertexHTTPSPort}))
	}
	return svcs
}

func (mv MonoVertex) getServiceObj(name string, headless bool, ports map[string]int32) *corev1.Service {
	var servicePorts []corev1.ServicePort
	portNames := make([]string, 0, len(ports))
	for k := range ports {
		portNames = append(portNames, k)
	}
	// Sort by name instead of iterating the map directly to make sure the genereated spec is consistent
	sort.Strings(portNames)
	for _, name := range portNames {
		port := ports[name]
		servicePorts = append(servicePorts, corev1.ServicePort{
			Port:       port,
			TargetPort: intstr.FromInt32(port),
			Name:       name,
		})
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       mv.Namespace,
			Name:            name,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(mv.GetObjectMeta(), MonoVertexGroupVersionKind)},
			Labels: map[string]string{
				KeyPartOf:         Project,
				KeyManagedBy:      ControllerMonoVertex,
				KeyComponent:      ComponentMonoVertex,
				KeyMonoVertexName: mv.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: servicePorts,
			Selector: map[string]string{
				KeyPartOf:         Project,
				KeyManagedBy:      ControllerMonoVertex,
				KeyComponent:      ComponentMonoVertex,
				KeyMonoVertexName: mv.Name,
			},
		},
	}
	if headless {
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.ClusterIP = "None"
	}
	return svc
}

func (mv MonoVertex) GetDaemonServiceName() string {
	return fmt.Sprintf("%s-mv-daemon-svc", mv.Name)
}

func (mv MonoVertex) GetDaemonDeploymentName() string {
	return fmt.Sprintf("%s-mv-daemon", mv.Name)
}

func (mv MonoVertex) GetDaemonServiceURL() string {
	// Note: the format of the URL is also used in `server/apis/v1/handler.go`
	// Do not change it without updating the handler.
	return fmt.Sprintf("%s.%s.svc:%d", mv.GetDaemonServiceName(), mv.Namespace, MonoVertexDaemonServicePort)
}

func (mv MonoVertex) Scalable() bool {
	return !mv.Spec.Scale.Disabled
}

func (mv MonoVertex) GetDaemonServiceObj() *corev1.Service {
	labels := map[string]string{
		KeyPartOf:         Project,
		KeyManagedBy:      ControllerMonoVertex,
		KeyComponent:      ComponentMonoVertexDaemon,
		KeyMonoVertexName: mv.Name,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mv.Namespace,
			Name:      mv.GetDaemonServiceName(),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mv.GetObjectMeta(), MonoVertexGroupVersionKind),
			},
			Labels: labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "tcp", Port: MonoVertexDaemonServicePort, TargetPort: intstr.FromInt32(MonoVertexDaemonServicePort)},
			},
			Selector: labels,
		},
	}
}

func (mv MonoVertex) GetDaemonDeploymentObj(req GetMonoVertexDaemonDeploymentReq) (*appv1.Deployment, error) {
	mvVtxCopyBytes, err := json.Marshal(mv.simpleCopy())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mono vertex spec")
	}
	encodedMonoVtx := base64.StdEncoding.EncodeToString(mvVtxCopyBytes)
	envVars := []corev1.EnvVar{
		{Name: EnvNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: EnvPod, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: EnvMonoVertexObject, Value: encodedMonoVtx},
	}
	envVars = append(envVars, req.Env...)
	c := corev1.Container{
		Ports:           []corev1.ContainerPort{{ContainerPort: MonoVertexDaemonServicePort}},
		Name:            CtrMain,
		Image:           req.Image,
		ImagePullPolicy: req.PullPolicy,
		Resources:       req.DefaultResources,
		Env:             envVars,
		Args:            []string{"mvtx-daemon-server"},
	}

	c.ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/readyz",
				Port:   intstr.FromInt32(MonoVertexDaemonServicePort),
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
				Port:   intstr.FromInt32(MonoVertexDaemonServicePort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}

	labels := map[string]string{
		KeyPartOf:         Project,
		KeyManagedBy:      ControllerMonoVertex,
		KeyComponent:      ComponentMonoVertexDaemon,
		KeyAppName:        mv.GetDaemonDeploymentName(),
		KeyMonoVertexName: mv.Name,
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
				Containers: []corev1.Container{c},
			},
		},
	}
	if dt := mv.Spec.DaemonTemplate; dt != nil {
		spec.Replicas = dt.Replicas
		dt.AbstractPodTemplate.ApplyToPodTemplateSpec(&spec.Template)
		if dt.ContainerTemplate != nil {
			dt.ContainerTemplate.ApplyToNumaflowContainers(spec.Template.Spec.Containers)
		}
	}
	return &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mv.Namespace,
			Name:      mv.GetDaemonDeploymentName(),
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mv.GetObjectMeta(), MonoVertexGroupVersionKind),
			},
		},
		Spec: spec,
	}, nil
}

// CommonEnvs returns the common envs for all mono vertex pod containers.
func (mv MonoVertex) commonEnvs() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: EnvNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: EnvPod, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: EnvReplica, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + KeyReplica + "']"}}},
		{Name: EnvMonoVertexName, Value: mv.Name},
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

func (mv MonoVertex) simpleCopy() MonoVertex {
	m := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mv.Namespace,
			Name:      mv.Name,
		},
		Spec: mv.Spec.DeepCopyWithoutReplicas(),
	}
	if m.Spec.Limits == nil {
		m.Spec.Limits = &MonoVertexLimits{}
	}
	if m.Spec.Limits.ReadBatchSize == nil {
		m.Spec.Limits.ReadBatchSize = ptr.To[uint64](DefaultReadBatchSize)
	}
	if m.Spec.Limits.ReadTimeout == nil {
		m.Spec.Limits.ReadTimeout = &metav1.Duration{Duration: DefaultReadTimeout}
	}
	m.Spec.UpdateStrategy = UpdateStrategy{}
	m.Spec.Lifecycle = MonoVertexLifecycle{}
	return m
}

func (mv MonoVertex) GetPodSpec(req GetMonoVertexPodSpecReq) (*corev1.PodSpec, error) {
	copiedSpec := mv.simpleCopy()
	copiedSpec.Spec.Scale = Scale{LookbackSeconds: ptr.To(uint32(mv.Spec.Scale.GetLookbackSeconds()))}
	monoVtxBytes, err := json.Marshal(copiedSpec)
	if err != nil {
		return nil, errors.New("failed to marshal mono vertex spec")
	}
	encodedMonoVertexSpec := base64.StdEncoding.EncodeToString(monoVtxBytes)
	envVars := []corev1.EnvVar{
		{Name: EnvMonoVertexObject, Value: encodedMonoVertexSpec},
	}
	envVars = append(envVars, mv.commonEnvs()...)
	envVars = append(envVars, req.Env...)

	varVolumeName := "var-run-numaflow"
	volumes := []corev1.Volume{
		{
			Name: varVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium: corev1.StorageMediumMemory,
			}},
		},
		{
			Name: RuntimeDirVolume,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				SizeLimit: resource.NewQuantity(RuntimeDirSizeLimit, resource.BinarySI),
			}},
		},
	}
	volumeMounts := []corev1.VolumeMount{
		{Name: varVolumeName, MountPath: PathVarRun},
		{Name: RuntimeDirVolume, MountPath: RuntimeDirMountPath},
	}
	containerRequest := getContainerReq{
		env:             envVars,
		image:           req.Image,
		imagePullPolicy: req.PullPolicy,
		resources:       req.DefaultResources,
		volumeMounts:    volumeMounts,
	}

	sidecarContainers, containers := mv.Spec.buildContainers(containerRequest)

	var readyzInitDeploy, readyzPeriodSeconds, readyzTimeoutSeconds, readyzFailureThreshold int32 = NumaContainerReadyzInitialDelaySeconds, NumaContainerReadyzPeriodSeconds, NumaContainerReadyzTimeoutSeconds, NumaContainerReadyzFailureThreshold
	var liveZInitDeploy, liveZPeriodSeconds, liveZTimeoutSeconds, liveZFailureThreshold int32 = NumaContainerLivezInitialDelaySeconds, NumaContainerLivezPeriodSeconds, NumaContainerLivezTimeoutSeconds, NumaContainerLivezFailureThreshold
	if x := mv.Spec.ContainerTemplate; x != nil {
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
				Port:   intstr.FromInt32(MonoVertexMetricsPort),
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
				Port:   intstr.FromInt32(MonoVertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: liveZInitDeploy,
		PeriodSeconds:       liveZPeriodSeconds,
		TimeoutSeconds:      liveZTimeoutSeconds,
		FailureThreshold:    liveZFailureThreshold,
	}
	containers[0].Ports = []corev1.ContainerPort{
		{Name: MonoVertexMetricsPortName, ContainerPort: MonoVertexMetricsPort},
	}

	for i := 0; i < len(sidecarContainers); i++ { // udsink, udsource, udtransformer ...
		sidecarContainers[i].Env = append(sidecarContainers[i].Env, mv.commonEnvs()...)

		// pass read limits as envs into UDSource container
		if sidecarContainers[i].Name == CtrUdsource {
			var bs uint64 = DefaultReadBatchSize
			toDur := DefaultReadTimeout
			if mv.Spec.Limits != nil {
				bs = mv.Spec.Limits.GetReadBatchSize()
				toDur = mv.Spec.Limits.GetReadTimeout()
			}
			sidecarContainers[i].Env = append(sidecarContainers[i].Env,
				corev1.EnvVar{Name: EnvReadBatchSize, Value: strconv.FormatUint(bs, 10)},
				corev1.EnvVar{Name: EnvReadTimeoutMs, Value: strconv.FormatInt(toDur.Milliseconds(), 10)},
			)
		}

	}

	initContainers := []corev1.Container{}
	initContainers = append(initContainers, mv.Spec.InitContainers...)
	// TODO: (k8s 1.29)  clean this up once we deprecate the support for k8s < 1.29
	if isSidecarSupported() {
		initContainers = append(initContainers, sidecarContainers...)
	} else {
		containers = append(containers, sidecarContainers...)
	}

	spec := &corev1.PodSpec{
		Subdomain:      mv.GetHeadlessServiceName(),
		Volumes:        append(volumes, mv.Spec.Volumes...),
		InitContainers: initContainers,
		Containers:     append(containers, mv.Spec.Sidecars...),
	}
	mv.Spec.AbstractPodTemplate.ApplyToPodSpec(spec)
	if mv.Spec.ContainerTemplate != nil {
		mv.Spec.ContainerTemplate.ApplyToNumaflowContainers(spec.Containers)
	}
	return spec, nil
}

type MonoVertexSpec struct {
	// +kubebuilder:default=1
	// +optional
	Replicas *int32  `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
	Source   *Source `json:"source,omitempty" protobuf:"bytes,2,opt,name=source"`
	Sink     *Sink   `json:"sink,omitempty" protobuf:"bytes,3,opt,name=sink"`
	// +optional
	UDF *UDF `json:"udf,omitempty" protobuf:"bytes,4,opt,name=udf"`
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,5,opt,name=abstractPodTemplate"`
	// Container template for the main numa container.
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,6,opt,name=containerTemplate"`
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=name
	Volumes []corev1.Volume `json:"volumes,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=volumes"`
	// Limits define the limitations such as read batch size for the mono vertex.
	// +optional
	Limits *MonoVertexLimits `json:"limits,omitempty" protobuf:"bytes,8,opt,name=limits"`
	// Settings for autoscaling
	// +optional
	Scale Scale `json:"scale,omitempty" protobuf:"bytes,9,opt,name=scale"`
	// List of customized init containers belonging to the pod.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
	// +optional
	InitContainers []corev1.Container `json:"initContainers,omitempty" protobuf:"bytes,10,rep,name=initContainers"`
	// List of customized sidecar containers belonging to the pod.
	// +optional
	Sidecars []corev1.Container `json:"sidecars,omitempty" protobuf:"bytes,11,rep,name=sidecars"`
	// Template for the daemon service deployment.
	// +optional
	DaemonTemplate *DaemonTemplate `json:"daemonTemplate,omitempty" protobuf:"bytes,12,opt,name=daemonTemplate"`
	// The strategy to use to replace existing pods with new ones.
	// +kubebuilder:default={"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": "25%"}}
	// +optional
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty" protobuf:"bytes,13,opt,name=updateStrategy"`
	// Lifecycle defines the Lifecycle properties of a MonoVertex
	// +kubebuilder:default={"desiredPhase": Running}
	// +optional
	Lifecycle MonoVertexLifecycle `json:"lifecycle,omitempty" protobuf:"bytes,14,opt,name=lifecycle"`
}

func (mvspec MonoVertexSpec) DeepCopyWithoutReplicas() MonoVertexSpec {
	x := *mvspec.DeepCopy()
	x.Replicas = ptr.To[int32](0)
	return x
}

func (mvspec MonoVertexSpec) getMainContainer(req getContainerReq) corev1.Container {
	return containerBuilder{}.
		init(req).command(NumaflowRustBinary).args("processor").build()
}

// buildContainers builds the sidecar containers and main containers for the mono vertex.
func (mvspec MonoVertexSpec) buildContainers(req getContainerReq) ([]corev1.Container, []corev1.Container) {
	mainContainer := mvspec.getMainContainer(req)
	containers := []corev1.Container{mainContainer}

	monitorContainer := buildMonitorContainer(req)
	sidecarContainers := []corev1.Container{monitorContainer}
	if mvspec.Source.UDSource != nil { // Only support UDSource for now.
		sidecarContainers = append(sidecarContainers, mvspec.Source.getUDSourceContainer(req))
	}
	if mvspec.UDF != nil {
		sidecarContainers = append(sidecarContainers, mvspec.UDF.getUDFContainer(req))
	}
	if mvspec.Source.UDTransformer != nil {
		sidecarContainers = append(sidecarContainers, mvspec.Source.getUDTransformerContainer(req))
	}
	if mvspec.Sink.UDSink != nil { // Only support UDSink for now.
		sidecarContainers = append(sidecarContainers, mvspec.Sink.getUDSinkContainer(req))
	}
	if mvspec.Sink.Fallback != nil && mvspec.Sink.Fallback.UDSink != nil {
		sidecarContainers = append(sidecarContainers, mvspec.Sink.getFallbackUDSinkContainer(req))
	}
	if mvspec.Sink.OnSuccess != nil && mvspec.Sink.OnSuccess.UDSink != nil {
		sidecarContainers = append(sidecarContainers, mvspec.Sink.getOnSuccessUDSinkContainer(req))
	}

	sidecarContainers = append(sidecarContainers, mvspec.Sidecars...)
	return sidecarContainers, containers
}

type MonoVertexLimits struct {
	// Read batch size from the source.
	// +kubebuilder:default=500
	// +optional
	ReadBatchSize *uint64 `json:"readBatchSize,omitempty" protobuf:"varint,1,opt,name=readBatchSize"`
	// ReadTimeout is the read timeout duration from the source.
	// +kubebuilder:default= "1s"
	// +optional
	ReadTimeout *metav1.Duration `json:"readTimeout,omitempty" protobuf:"bytes,2,opt,name=readTimeout"`
	// RateLimit for MonoVertex defines how many messages can be read from Source. This is computed by number of
	// `read` calls per second multiplied by the `readBatchSize`. This is how RateLimit is calculated for MonoVertex and
	// for Source vertices.
	// +optional
	RateLimit *RateLimit `json:"rateLimit,omitempty" protobuf:"bytes,3,opt,name=rateLimit"`
}

func (mvl MonoVertexLimits) GetReadBatchSize() uint64 {
	if mvl.ReadBatchSize == nil {
		return DefaultReadBatchSize
	}
	return *mvl.ReadBatchSize
}

func (mvl MonoVertexLimits) GetReadTimeout() time.Duration {
	if mvl.ReadTimeout == nil {
		return DefaultReadTimeout
	}
	return mvl.ReadTimeout.Duration
}

type MonoVertexStatus struct {
	Status `json:",inline" protobuf:"bytes,1,opt,name=status"`
	// +optional
	Phase MonoVertexPhase `json:"phase,omitempty" protobuf:"bytes,2,opt,name=phase,casttype=MonoVertexPhase"`
	// Total number of non-terminated pods targeted by this MonoVertex (their labels match the selector).
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
	// +optional
	LastUpdated metav1.Time `json:"lastUpdated,omitempty" protobuf:"bytes,8,opt,name=lastUpdated"`
	// Time of last scaling operation.
	// +optional
	LastScaledAt metav1.Time `json:"lastScaledAt,omitempty" protobuf:"bytes,9,opt,name=lastScaledAt"`
	// The generation observed by the MonoVertex controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,10,opt,name=observedGeneration"`
	// The number of pods targeted by this MonoVertex with a Ready Condition.
	// +optional
	ReadyReplicas uint32 `json:"readyReplicas,omitempty" protobuf:"varint,11,opt,name=readyReplicas"`
	// The number of Pods created by the controller from the MonoVertex version indicated by updateHash.
	UpdatedReplicas uint32 `json:"updatedReplicas,omitempty" protobuf:"varint,12,opt,name=updatedReplicas"`
	// The number of ready Pods created by the controller from the MonoVertex version indicated by updateHash.
	UpdatedReadyReplicas uint32 `json:"updatedReadyReplicas,omitempty" protobuf:"varint,13,opt,name=updatedReadyReplicas"`
	// If not empty, indicates the current version of the MonoVertex used to generate Pods.
	CurrentHash string `json:"currentHash,omitempty" protobuf:"bytes,14,opt,name=currentHash"`
	// If not empty, indicates the updated version of the MonoVertex used to generate Pods.
	UpdateHash string `json:"updateHash,omitempty" protobuf:"bytes,15,opt,name=updateHash"`
}

// SetObservedGeneration sets the Status ObservedGeneration
func (mvs *MonoVertexStatus) SetObservedGeneration(value int64) {
	mvs.ObservedGeneration = value
}

// InitConditions sets conditions to Unknown state.
func (mvs *MonoVertexStatus) InitConditions() {
	mvs.InitializeConditions(MonoVertexConditionDeployed, MonoVertexConditionDaemonHealthy, MonoVertexPodsHealthy)
}

// MarkDeployed set the MonoVertex has it's sub resources deployed.
func (mvs *MonoVertexStatus) MarkDeployed() {
	mvs.MarkTrue(MonoVertexConditionDeployed)
}

// MarkDeployFailed set the MonoVertex deployment failed
func (mvs *MonoVertexStatus) MarkDeployFailed(reason, message string) {
	mvs.MarkFalse(MonoVertexConditionDeployed, reason, message)
	mvs.MarkPhaseFailed(reason, message)
}

// MarkDaemonHealthy set the daemon service of the mono vertex is healthy.
func (mvs *MonoVertexStatus) MarkDaemonHealthy() {
	mvs.MarkTrue(MonoVertexConditionDaemonHealthy)
}

// MarkDaemonUnHealthy set the daemon service of the mono vertex is unhealthy.
func (mvs *MonoVertexStatus) MarkDaemonUnHealthy(reason, message string) {
	mvs.MarkFalse(MonoVertexConditionDaemonHealthy, reason, message)
	mvs.Message = "Degraded: " + message
}

// MarkPodHealthy marks the pod as healthy with the given reason and message.
func (mvs *MonoVertexStatus) MarkPodHealthy(reason, message string) {
	mvs.MarkTrueWithReason(MonoVertexPodsHealthy, reason, message)
}

// MarkPodNotHealthy marks the pod not healthy with the given reason and message.
func (mvs *MonoVertexStatus) MarkPodNotHealthy(reason, message string) {
	mvs.MarkFalse(MonoVertexPodsHealthy, reason, message)
	mvs.Reason = reason
	mvs.Message = "Degraded: " + message
}

// MarkPhase marks the phase with the given reason and message.
func (mvs *MonoVertexStatus) MarkPhase(phase MonoVertexPhase, reason, message string) {
	mvs.Phase = phase
	mvs.Reason = reason
	mvs.Message = message
}

// MarkPhaseFailed marks the phase as failed with the given reason and message.
func (mvs *MonoVertexStatus) MarkPhaseFailed(reason, message string) {
	mvs.MarkPhase(MonoVertexPhaseFailed, reason, message)
}

// MarkPhaseRunning marks the phase as running.
func (mvs *MonoVertexStatus) MarkPhaseRunning() {
	mvs.MarkPhase(MonoVertexPhaseRunning, "", "")
}

// MarkPhasePaused set the MonoVertex has been paused.
func (mvs *MonoVertexStatus) MarkPhasePaused() {
	mvs.MarkPhase(MonoVertexPhasePaused, "", "MonoVertex paused")
}

// IsHealthy indicates whether the MonoVertex is in healthy status
// It returns false if any issues exists
// True indicates that the MonoVertex is healthy
func (mvs *MonoVertexStatus) IsHealthy() bool {
	// check for the phase field first
	switch mvs.Phase {
	// Directly return an error if the phase is failed
	case MonoVertexPhaseFailed:
		return false
	// Check if the MonoVertex is ready if the phase is running or Paused,
	// We check if all the required conditions are true for it to be healthy
	case MonoVertexPhaseRunning, MonoVertexPhasePaused:
		return mvs.IsReady()
	default:
		return false
	}
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MonoVertexList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []MonoVertex `json:"items" protobuf:"bytes,2,rep,name=items"`
}

type MonoVertexLifecycle struct {
	// DesiredPhase used to bring the MonoVertex from current phase to desired phase
	// +kubebuilder:default=Running
	// +optional
	DesiredPhase MonoVertexPhase `json:"desiredPhase,omitempty" protobuf:"bytes,1,opt,name=desiredPhase"`
}

// GetDesiredPhase is used to fetch the desired lifecycle phase for a MonoVertex
func (lc MonoVertexLifecycle) GetDesiredPhase() MonoVertexPhase {
	switch lc.DesiredPhase {
	case MonoVertexPhasePaused:
		return MonoVertexPhasePaused
	default:
		return MonoVertexPhaseRunning
	}
}
