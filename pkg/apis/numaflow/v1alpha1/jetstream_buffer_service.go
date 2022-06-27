package v1alpha1

import (
	fmt "fmt"
	"strconv"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

type JetStreamBufferService struct {
	// JetStream version, such as "2.7.1"
	Version string `json:"version,omitempty" protobuf:"bytes,1,opt,name=version"`
	// Redis StatefulSet size
	// +kubebuilder:default=3
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`
	// ContainerTemplate contains customized spec for NATS container
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,3,opt,name=containerTemplate"`
	// ReloaderContainerTemplate contains customized spec for config reloader container
	// +optional
	ReloaderContainerTemplate *ContainerTemplate `json:"reloaderContainerTemplate,omitempty" protobuf:"bytes,4,opt,name=reloaderContainerTemplate"`
	// MetricsContainerTemplate contains customized spec for metrics container
	// +optional
	MetricsContainerTemplate *ContainerTemplate `json:"metricsContainerTemplate,omitempty" protobuf:"bytes,5,opt,name=metricsContainerTemplate"`
	// +optional
	Persistence *PersistenceStrategy `json:"persistence,omitempty" protobuf:"bytes,6,opt,name=persistence"`
	// Metadata sets the pods's metadata, i.e. annotations and labels
	Metadata *Metadata `json:"metadata,omitempty" protobuf:"bytes,77,opt,name=metadata"`
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
	// JetStream configuration, if not specified, global settings in numaflow-controller-config will be used.
	// See https://docs.nats.io/running-a-nats-service/configuration#jetstream.
	// Only configure "max_memory_store" or "max_file_store", do not set "store_dir" as it has been hardcoded.
	// +optional
	Settings *string `json:"settings,omitempty" protobuf:"bytes,16,opt,name=settings"`
	// Optional arguments to start nats-server. For example, "-D" to enable debugging output, "-DV" to enable debugging and tracing.
	// Check https://docs.nats.io/ for all the available arguments.
	// +optional
	StartArgs []string `json:"startArgs,omitempty" protobuf:"bytes,17,rep,name=startArgs"`
	// Optional configuration for the streams, consumers and buckets to be created in this JetStream service, if specified, it will be merged with the default configuration in numaflow-controller-config.
	// It accepts a YAML format configuration, it may include 4 sections, "stream", "consumer", "otBucket" and "procBucket".
	// Available fields under "stream" include "retention" (e.g. interest, limits, workerQueue), "maxMsgs", "maxAge" (e.g. 72h), "replicas" (1, 3, 5), "duplicates" (e.g. 5m).
	// Available fields under "consumer" include "ackWait" (e.g. 60s)
	// Available fields under "otBucket" include "maxValueSize", "history", "ttl" (e.g. 72h), "maxBytes", "replicas" (1, 3, 5).
	// Available fields under "procBucket" include "maxValueSize", "history", "ttl" (e.g. 72h), "maxBytes", "replicas" (1, 3, 5).
	// +optional
	BufferConfig *string `json:"bufferConfig,omitempty" protobuf:"bytes,18,opt,name=bufferConfig"`
	// Whether encrypt the data at rest, defaults to false
	// Enabling encryption might impact the performance, see https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/encryption_at_rest for the detail
	// Toggling the value will impact encypting/decrypting existing messages.
	// +optional
	Encryption bool `json:"encryption,omitempty" protobuf:"bytes,19,opt,name=encryption"`
	// Whether enable TLS, defaults to false
	// Enabling TLS might impact the performance
	// +optional
	TLS bool `json:"tls,omitempty" protobuf:"bytes,20,opt,name=tls"`
}

func (j JetStreamBufferService) GetReplicas() int {
	if j.Replicas == nil {
		return 3
	}
	if *j.Replicas < 3 {
		return 3
	}
	return int(*j.Replicas)
}

func (j JetStreamBufferService) GetServiceSpec(req GetJetStreamServiceSpecReq) corev1.ServiceSpec {
	return corev1.ServiceSpec{
		Ports: []corev1.ServicePort{
			{Name: "tcp-client", Port: req.ClientPort},
			{Name: "cluster", Port: req.ClusterPort},
			{Name: "metrics", Port: req.MetricsPort},
			{Name: "monitor", Port: req.MonitorPort},
		},
		Type:                     corev1.ServiceTypeClusterIP,
		ClusterIP:                corev1.ClusterIPNone,
		PublishNotReadyAddresses: true,
		Selector:                 req.Labels,
	}
}

func (j JetStreamBufferService) GetStatefulSetSpec(req GetJetStreamStatefulSetSpecReq) appv1.StatefulSetSpec {
	replicas := int32(j.GetReplicas())
	podTemplateLabels := make(map[string]string)
	if j.Metadata != nil &&
		len(j.Metadata.Labels) > 0 {
		for k, v := range j.Metadata.Labels {
			podTemplateLabels[k] = v
		}
	}
	for k, v := range req.Labels {
		podTemplateLabels[k] = v
	}
	var jsContainerPullPolicy, reloaderContainerPullPolicy, metricsContainerPullPolicy corev1.PullPolicy
	var jsContainerSecurityContext, reloaderContainerSecurityContext, metricsContainerSecurityContext *corev1.SecurityContext
	if j.ContainerTemplate != nil {
		jsContainerPullPolicy = j.ContainerTemplate.ImagePullPolicy
		jsContainerSecurityContext = j.ContainerTemplate.SecurityContext
	}
	if j.ReloaderContainerTemplate != nil {
		reloaderContainerPullPolicy = j.ReloaderContainerTemplate.ImagePullPolicy
		reloaderContainerSecurityContext = j.ReloaderContainerTemplate.SecurityContext
	}
	if j.MetricsContainerTemplate != nil {
		metricsContainerPullPolicy = j.MetricsContainerTemplate.ImagePullPolicy
		metricsContainerSecurityContext = j.MetricsContainerTemplate.SecurityContext
	}
	projectedSecretKeyToPaths := []corev1.KeyToPath{
		{
			Key:  JetStreamServerSecretAuthKey,
			Path: "auth.conf",
		},
	}
	if j.TLS {
		projectedSecretKeyToPaths = append(projectedSecretKeyToPaths, corev1.KeyToPath{
			Key:  JetStreamServerPrivateKeyKey,
			Path: "server-key.pem",
		})
		projectedSecretKeyToPaths = append(projectedSecretKeyToPaths, corev1.KeyToPath{
			Key:  JetStreamServerCertKey,
			Path: "server-cert.pem",
		})
		projectedSecretKeyToPaths = append(projectedSecretKeyToPaths, corev1.KeyToPath{
			Key:  JetStreamServerCACertKey,
			Path: "ca-cert.pem",
		})
		projectedSecretKeyToPaths = append(projectedSecretKeyToPaths, corev1.KeyToPath{
			Key:  JetStreamClusterPrivateKeyKey,
			Path: "cluster-server-key.pem",
		})
		projectedSecretKeyToPaths = append(projectedSecretKeyToPaths, corev1.KeyToPath{
			Key:  JetStreamClusterCertKey,
			Path: "cluster-server-cert.pem",
		})
		projectedSecretKeyToPaths = append(projectedSecretKeyToPaths, corev1.KeyToPath{
			Key:  JetStreamClusterCACertKey,
			Path: "cluster-ca-cert.pem",
		})
	}
	spec := appv1.StatefulSetSpec{
		PodManagementPolicy: appv1.ParallelPodManagement,
		Replicas:            &replicas,
		ServiceName:         req.ServiceName,
		Selector: &metav1.LabelSelector{
			MatchLabels: req.Labels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podTemplateLabels,
			},
			Spec: corev1.PodSpec{
				NodeSelector:                  j.NodeSelector,
				Tolerations:                   j.Tolerations,
				SecurityContext:               j.SecurityContext,
				ImagePullSecrets:              j.ImagePullSecrets,
				PriorityClassName:             j.PriorityClassName,
				Priority:                      j.Priority,
				ServiceAccountName:            j.ServiceAccountName,
				Affinity:                      j.Affinity,
				ShareProcessNamespace:         pointer.Bool(true),
				TerminationGracePeriodSeconds: pointer.Int64(60),
				Volumes: []corev1.Volume{
					{Name: "pid", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					{
						Name: "config-volume",
						VolumeSource: corev1.VolumeSource{
							Projected: &corev1.ProjectedVolumeSource{
								Sources: []corev1.VolumeProjection{
									{
										ConfigMap: &corev1.ConfigMapProjection{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: req.ConfigMapName,
											},
											Items: []corev1.KeyToPath{
												{
													Key:  JetStreamConfigMapKey,
													Path: "nats-js.conf",
												},
											},
										},
									},
									{
										Secret: &corev1.SecretProjection{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: req.ServerAuthSecretName,
											},
											Items: projectedSecretKeyToPaths,
										},
									},
								},
							},
						},
					},
				},
				Containers: []corev1.Container{
					{
						Name:            "main",
						Image:           req.NatsImage,
						ImagePullPolicy: jsContainerPullPolicy,
						Ports: []corev1.ContainerPort{
							{Name: "client", ContainerPort: req.ClientPort},
							{Name: "cluster", ContainerPort: req.ClusterPort},
							{Name: "monitor", ContainerPort: req.MonitorPort},
						},
						Command: []string{req.StartCommand, "--config", "/etc/nats-config/nats-js.conf"},
						Args:    j.StartArgs,
						Env: []corev1.EnvVar{
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
							{Name: "SERVER_NAME", Value: "$(POD_NAME)"},
							{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
							{Name: "CLUSTER_ADVERTISE", Value: "$(POD_NAME)." + req.ServiceName + ".$(POD_NAMESPACE).svc.cluster.local"},
							{Name: "JS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: req.ServerEncryptionSecretName}, Key: JetStreamServerSecretEncryptionKey}}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config-volume", MountPath: "/etc/nats-config"},
							{Name: "pid", MountPath: "/var/run/nats"},
						},
						SecurityContext: jsContainerSecurityContext,
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(req.MonitorPort)),
								},
							},
							FailureThreshold:    30,
							InitialDelaySeconds: 10,
							TimeoutSeconds:      5,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/",
									Port: intstr.FromInt(int(req.MonitorPort)),
								},
							},
							InitialDelaySeconds: 10,
							PeriodSeconds:       30,
							TimeoutSeconds:      5,
						},
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{
								Exec: &corev1.ExecAction{
									Command: []string{req.StartCommand, "-sl=ldm=/var/run/nats/nats.pid"},
								},
							},
						},
					},
					{
						Name:            "reloader",
						Image:           req.ConfigReloaderImage,
						ImagePullPolicy: reloaderContainerPullPolicy,
						SecurityContext: reloaderContainerSecurityContext,
						Command:         []string{"nats-server-config-reloader", "-pid", "/var/run/nats/nats.pid", "-config", "/etc/nats-config/nats-js.conf"},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config-volume", MountPath: "/etc/nats-config"},
							{Name: "pid", MountPath: "/var/run/nats"},
						},
					},
					{
						Name:            "metrics",
						Image:           req.MetricsExporterImage,
						ImagePullPolicy: metricsContainerPullPolicy,
						Ports: []corev1.ContainerPort{
							{Name: "metrics", ContainerPort: req.MetricsPort},
						},
						Args:            []string{"-connz", "-routez", "-subz", "-varz", "-prefix=nats", "-use_internal_server_id", "-jsz=all", fmt.Sprintf("http://localhost:%s", strconv.Itoa(int(req.MonitorPort)))},
						SecurityContext: metricsContainerSecurityContext,
					},
				},
			},
		},
	}
	if j.Metadata != nil {
		spec.Template.SetAnnotations(j.Metadata.Annotations)
	}
	if j.ContainerTemplate != nil {
		spec.Template.Spec.Containers[0].Resources = j.ContainerTemplate.Resources
	}
	if j.MetricsContainerTemplate != nil {
		spec.Template.Spec.Containers[1].Resources = j.MetricsContainerTemplate.Resources
	}
	if j.Persistence != nil {
		volMode := corev1.PersistentVolumeFilesystem
		// Default volume size
		volSize := apiresource.MustParse("20Gi")
		if j.Persistence.VolumeSize != nil {
			volSize = *j.Persistence.VolumeSize
		}
		// Default to ReadWriteOnce
		accessMode := corev1.ReadWriteOnce
		if j.Persistence.AccessMode != nil {
			accessMode = *j.Persistence.AccessMode
		}
		spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: req.PvcNameIfNeeded,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						accessMode,
					},
					VolumeMode:       &volMode,
					StorageClassName: j.Persistence.StorageClassName,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: volSize,
						},
					},
				},
			},
		}
		volumeMounts := spec.Template.Spec.Containers[0].VolumeMounts
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: req.PvcNameIfNeeded, MountPath: "/data/jetstream"})
		spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
	} else {
		// When the POD is runasnonroot, it can not create the dir /data/jetstream
		// Use an emptyDirVolume
		emptyDirVolName := "js-data"
		volumes := spec.Template.Spec.Volumes
		volumes = append(volumes, corev1.Volume{Name: emptyDirVolName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		spec.Template.Spec.Volumes = volumes
		volumeMounts := spec.Template.Spec.Containers[0].VolumeMounts
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: emptyDirVolName, MountPath: "/data/jetstream"})
		spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
	}
	return spec
}

type JetStreamConfig struct {
	// JetStream (NATS) URL
	URL  string    `json:"url,omitempty" protobuf:"bytes,1,opt,name=url"`
	Auth *NATSAuth `json:"auth,omitempty" protobuf:"bytes,2,opt,name=auth"`
	// +optional
	BufferConfig string `json:"bufferConfig,omitempty" protobuf:"bytes,3,opt,name=bufferConfig"`
	// TLS enabled or not
	TLSEnabled bool `json:"tlsEnabled,omitempty" protobuf:"bytes,4,opt,name=tlsEnabled"`
}

type NATSAuth struct {
	// Secret for auth user
	// +optional
	User *corev1.SecretKeySelector `json:"user,omitempty" protobuf:"bytes,1,opt,name=user"`
	// Secret for auth password
	// +optional
	Password *corev1.SecretKeySelector `json:"password,omitempty" protobuf:"bytes,2,opt,name=password"`
}
