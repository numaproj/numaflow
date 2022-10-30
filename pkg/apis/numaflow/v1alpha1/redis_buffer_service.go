package v1alpha1

import (
	"fmt"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RedisBufferService struct {
	// Native brings up a native Redis service
	Native *NativeRedis `json:"native,omitempty" protobuf:"bytes,1,opt,name=native"`
	// External holds an External Redis config
	External *RedisConfig `json:"external,omitempty" protobuf:"bytes,2,opt,name=external"`
}

type RedisConfig struct {
	// Redis URL
	// +optional
	URL string `json:"url,omitempty" protobuf:"bytes,1,opt,name=url"`
	// Sentinel URL, will be ignored if Redis URL is provided
	// +optional
	SentinelURL string `json:"sentinelUrl,omitempty" protobuf:"bytes,2,opt,name=sentinelUrl"`
	// Only required when Sentinel is used
	// +optional
	MasterName string `json:"masterName,omitempty" protobuf:"bytes,3,opt,name=masterName"`
	// Redis user
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,4,opt,name=user"`
	// Redis password secret selector
	// +optional
	Password *corev1.SecretKeySelector `json:"password,omitempty" protobuf:"bytes,5,opt,name=password"`
	// Sentinel password secret selector
	// +optional
	SentinelPassword *corev1.SecretKeySelector `json:"sentinelPassword,omitempty" protobuf:"bytes,6,opt,name=sentinelPassword"`
}

type NativeRedis struct {
	// Redis version, such as "6.0.16"
	Version string `json:"version,omitempty" protobuf:"bytes,1,opt,name=version"`
	// Redis StatefulSet size
	// +kubebuilder:default=3
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,2,opt,name=replicas"`
	// RedisContainerTemplate contains customized spec for Redis container
	// +optional
	RedisContainerTemplate *ContainerTemplate `json:"redisContainerTemplate,omitempty" protobuf:"bytes,3,opt,name=redisContainerTemplate"`
	// SentinelContainerTemplate contains customized spec for Redis container
	// +optional
	SentinelContainerTemplate *ContainerTemplate `json:"sentinelContainerTemplate,omitempty" protobuf:"bytes,4,opt,name=sentinelContainerTemplate"`
	// MetricsContainerTemplate contains customized spec for metrics container
	// +optional
	MetricsContainerTemplate *ContainerTemplate `json:"metricsContainerTemplate,omitempty" protobuf:"bytes,5,opt,name=metricsContainerTemplate"`
	// +optional
	Persistence *PersistenceStrategy `json:"persistence,omitempty" protobuf:"bytes,6,opt,name=persistence"`
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
	// Redis configuration, if not specified, global settings in numaflow-controller-config will be used.
	// +optional
	Settings *RedisSettings `json:"settings,omitempty" protobuf:"bytes,16,opt,name=settings"`
}

type RedisSettings struct {
	// Redis settings shared by both master and slaves, will override the global settings from controller config
	// +optional
	Redis string `json:"redis,omitempty" protobuf:"bytes,1,opt,name=redis"`
	// Special settings for Redis master node, will override the global settings from controller config
	// +optional
	Master string `json:"master,omitempty" protobuf:"bytes,2,opt,name=master"`
	// Special settings for Redis replica nodes, will override the global settings from controller config
	// +optional
	Replica string `json:"replica,omitempty" protobuf:"bytes,3,opt,name=replica"`
	// Sentinel settings, will override the global settings from controller config
	// +optional
	Sentinel string `json:"sentinel,omitempty" protobuf:"bytes,4,opt,name=sentinel"`
}

func (nr NativeRedis) GetReplicas() int {
	if nr.Replicas == nil {
		return 3
	}
	if *nr.Replicas < 3 {
		return 3
	}
	return int(*nr.Replicas)
}

func (nr NativeRedis) GetServiceSpec(req GetRedisServiceSpecReq) corev1.ServiceSpec {
	return corev1.ServiceSpec{
		Ports: []corev1.ServicePort{
			{Name: "tcp-redis", Port: req.RedisContainerPort},
			{Name: "tcp-sentinel", Port: req.SentinelContainerPort},
		},
		Type:     corev1.ServiceTypeClusterIP,
		Selector: req.Labels,
	}
}

func (nr NativeRedis) GetHeadlessServiceSpec(req GetRedisServiceSpecReq) corev1.ServiceSpec {
	spec := nr.GetServiceSpec(req)
	spec.ClusterIP = corev1.ClusterIPNone
	return spec
}

func (nr NativeRedis) GetStatefulSetSpec(req GetRedisStatefulSetSpecReq) appv1.StatefulSetSpec {
	replicas := int32(nr.GetReplicas())
	podTemplateLabels := make(map[string]string)
	if nr.Metadata != nil &&
		len(nr.Metadata.Labels) > 0 {
		for k, v := range nr.Metadata.Labels {
			podTemplateLabels[k] = v
		}
	}
	for k, v := range req.Labels {
		podTemplateLabels[k] = v
	}
	var redisContainerPullPolicy, sentinelContainerPullPolicy, metricsContainerPullPolicy corev1.PullPolicy
	if nr.RedisContainerTemplate != nil {
		redisContainerPullPolicy = nr.RedisContainerTemplate.ImagePullPolicy
	}
	if nr.SentinelContainerTemplate != nil {
		sentinelContainerPullPolicy = nr.SentinelContainerTemplate.ImagePullPolicy
	}
	if nr.MetricsContainerTemplate != nil {
		metricsContainerPullPolicy = nr.MetricsContainerTemplate.ImagePullPolicy
	}
	spec := appv1.StatefulSetSpec{
		Replicas:    &replicas,
		ServiceName: req.ServiceName,
		Selector: &metav1.LabelSelector{
			MatchLabels: req.Labels,
		},
		PodManagementPolicy: appv1.OrderedReadyPodManagement,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podTemplateLabels,
			},
			Spec: corev1.PodSpec{
				NodeSelector:       nr.NodeSelector,
				Tolerations:        nr.Tolerations,
				SecurityContext:    nr.SecurityContext,
				ImagePullSecrets:   nr.ImagePullSecrets,
				PriorityClassName:  nr.PriorityClassName,
				Priority:           nr.Priority,
				ServiceAccountName: nr.ServiceAccountName,
				Affinity:           nr.Affinity,
				Volumes: []corev1.Volume{
					{
						Name: "start-scripts",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: req.ScriptsConfigMapName,
								},
								DefaultMode: func(i int32) *int32 { return &i }(0x1ED),
							},
						},
					},
					{
						Name: "health",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: req.HealthConfigMapName,
								},
								DefaultMode: func(i int32) *int32 { return &i }(0x1ED),
							},
						},
					},
					{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: req.ConfConfigMapName,
								},
								DefaultMode: func(i int32) *int32 { return &i }(0x1ED),
							},
						},
					},
					{Name: "sentinel-tmp-conf", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					{Name: "redis-tmp-conf", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				},
				Containers: []corev1.Container{
					{
						Name:            "redis",
						Image:           req.RedisImage,
						ImagePullPolicy: redisContainerPullPolicy,
						Ports: []corev1.ContainerPort{
							{Name: "redis", ContainerPort: req.RedisContainerPort},
						},
						Command: []string{"/bin/bash"},
						Args:    []string{"-c", "/opt/bitnami/scripts/start-scripts/start-node.sh"},
						Env: []corev1.EnvVar{
							{Name: "BITNAMI_DEBUG", Value: "true"},
							{Name: "REDIS_MASTER_PORT_NUMBER", Value: fmt.Sprint(req.RedisContainerPort)},
							{Name: "ALLOW_EMPTY_PASSWORD", Value: "no"},
							{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: req.CredentialSecretName}, Key: RedisAuthSecretKey}}},
							{Name: "REDIS_MASTER_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: req.CredentialSecretName}, Key: RedisAuthSecretKey}}},
							{Name: "REDIS_TLS_ENABLED", Value: "no"},
							{Name: "REDIS_PORT", Value: fmt.Sprint(req.RedisContainerPort)},
							{Name: "REDIS_DATA_DIR", Value: "/data"},
						},
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"/bin/bash", "-c", "/opt/bitnami/scripts/start-scripts/prestop-redis.sh"},
								},
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "start-scripts", MountPath: "/opt/bitnami/scripts/start-scripts"},
							{Name: "health", MountPath: "/health"},
							{Name: "config", MountPath: "/opt/bitnami/redis/mounted-etc"},
							{Name: "redis-tmp-conf", MountPath: "/opt/bitnami/redis/etc"},
							{Name: "tmp", MountPath: "/tmp"},
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"sh", "-c", "/health/ping_liveness_local.sh 5"},
								},
							},
							InitialDelaySeconds: 20,
							TimeoutSeconds:      5,
							FailureThreshold:    5,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"sh", "-c", "/health/ping_readiness_local.sh 5"},
								},
							},
							InitialDelaySeconds: 20,
							TimeoutSeconds:      5,
							FailureThreshold:    5,
						},
					},
					{
						Name:            "sentinel",
						Image:           req.SentinelImage,
						ImagePullPolicy: sentinelContainerPullPolicy,
						Ports: []corev1.ContainerPort{
							{Name: "sentinel", ContainerPort: req.SentinelContainerPort},
						},
						Command: []string{"/bin/bash"},
						Args:    []string{"-c", "/opt/bitnami/scripts/start-scripts/start-sentinel.sh"},
						Env: []corev1.EnvVar{
							{Name: "BITNAMI_DEBUG", Value: "false"},
							{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: req.CredentialSecretName}, Key: RedisAuthSecretKey}}},
							{Name: "REDIS_SENTINEL_TLS_ENABLED", Value: "no"},
							{Name: "REDIS_SENTINEL_PORT", Value: fmt.Sprint(req.SentinelContainerPort)},
						},
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"/bin/bash", "-c", "/opt/bitnami/scripts/start-scripts/prestop-sentinel.sh"},
								},
							},
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"sh", "-c", "/health/ping_sentinel.sh 5"},
								},
							},
							InitialDelaySeconds: 20,
							TimeoutSeconds:      5,
							FailureThreshold:    5,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"sh", "-c", "/health/ping_sentinel.sh 5"},
								},
							},
							InitialDelaySeconds: 20,
							TimeoutSeconds:      5,
							FailureThreshold:    5,
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "start-scripts", MountPath: "/opt/bitnami/scripts/start-scripts"},
							{Name: "health", MountPath: "/health"},
							{Name: "config", MountPath: "/opt/bitnami/redis-sentinel/mounted-etc"},
							{Name: "sentinel-tmp-conf", MountPath: "/opt/bitnami/redis-sentinel/etc"},
						},
					},
					{
						Name:            "metrics",
						Image:           req.MetricsExporterImage,
						ImagePullPolicy: metricsContainerPullPolicy,
						Command: []string{"/bin/bash", "-c", `if [[ -f '/secrets/redis-password' ]]; then
  export REDIS_PASSWORD=$(cat /secrets/redis-password)
fi
redis_exporter`},
						Ports: []corev1.ContainerPort{
							{Name: "metrics", ContainerPort: req.RedisMetricsContainerPort},
						},
						Env: []corev1.EnvVar{
							{Name: "REDIS_ALIAS", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
							{Name: "REDIS_USER", Value: "default"},
							{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: req.CredentialSecretName}, Key: RedisAuthSecretKey}}},
						},
					},
				},
			},
		},
	}
	if nr.Metadata != nil {
		spec.Template.SetAnnotations(nr.Metadata.Annotations)
	}
	if nr.RedisContainerTemplate != nil {
		spec.Template.Spec.Containers[0].Resources = nr.RedisContainerTemplate.Resources
	}
	if nr.SentinelContainerTemplate != nil {
		spec.Template.Spec.Containers[1].Resources = nr.SentinelContainerTemplate.Resources
	}
	if nr.MetricsContainerTemplate != nil {
		spec.Template.Spec.Containers[2].Resources = nr.MetricsContainerTemplate.Resources
	}
	if nr.Persistence != nil {
		volMode := corev1.PersistentVolumeFilesystem
		// Default volume size
		volSize := apiresource.MustParse("20Gi")
		if nr.Persistence.VolumeSize != nil {
			volSize = *nr.Persistence.VolumeSize
		}
		// Default to ReadWriteOnce
		accessMode := corev1.ReadWriteOnce
		if nr.Persistence.AccessMode != nil {
			accessMode = *nr.Persistence.AccessMode
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
					StorageClassName: nr.Persistence.StorageClassName,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: volSize,
						},
					},
				},
			},
		}
		vm0s := spec.Template.Spec.Containers[0].VolumeMounts
		vm0s = append(vm0s, corev1.VolumeMount{Name: req.PvcNameIfNeeded, MountPath: "/data"})
		spec.Template.Spec.Containers[0].VolumeMounts = vm0s
		vm1s := spec.Template.Spec.Containers[1].VolumeMounts
		vm1s = append(vm1s, corev1.VolumeMount{Name: req.PvcNameIfNeeded, MountPath: "/data"})
		spec.Template.Spec.Containers[1].VolumeMounts = vm1s

		// volume permission
		runAsUser := int64(1001)
		fsGroup := int64(1001)
		runAsUser0 := int64(0)
		spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsUser: &runAsUser}
		spec.Template.Spec.Containers[1].SecurityContext = &corev1.SecurityContext{RunAsUser: &runAsUser}
		spec.Template.Spec.InitContainers = []corev1.Container{
			{
				Name:            "volume-permissions",
				Resources:       standardResources,
				SecurityContext: &corev1.SecurityContext{RunAsUser: &runAsUser0},
				VolumeMounts:    []corev1.VolumeMount{{Name: req.PvcNameIfNeeded, MountPath: "/data"}},
				Image:           req.InitContainerImage,
				Command:         []string{"/bin/bash", "-ec", "chown -R 1001:1001 /data"},
			},
		}
		if spec.Template.Spec.SecurityContext == nil {
			spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{}
		}
		spec.Template.Spec.SecurityContext.FSGroup = &fsGroup
	} else {
		emptyDirVolName := "redis-data"
		volumes := spec.Template.Spec.Volumes
		volumes = append(volumes, corev1.Volume{Name: emptyDirVolName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		spec.Template.Spec.Volumes = volumes
		vm0s := spec.Template.Spec.Containers[0].VolumeMounts
		vm0s = append(vm0s, corev1.VolumeMount{Name: emptyDirVolName, MountPath: "/data"})
		spec.Template.Spec.Containers[0].VolumeMounts = vm0s
		vm1s := spec.Template.Spec.Containers[1].VolumeMounts
		vm1s = append(vm1s, corev1.VolumeMount{Name: emptyDirVolName, MountPath: "/data"})
		spec.Template.Spec.Containers[1].VolumeMounts = vm1s
	}
	return spec
}
