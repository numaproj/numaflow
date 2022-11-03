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
	fmt "fmt"
	"strconv"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,7,opt,name=abstractPodTemplate"`
	// JetStream configuration, if not specified, global settings in numaflow-controller-config will be used.
	// See https://docs.nats.io/running-a-nats-service/configuration#jetstream.
	// Only configure "max_memory_store" or "max_file_store", do not set "store_dir" as it has been hardcoded.
	// +optional
	Settings *string `json:"settings,omitempty" protobuf:"bytes,8,opt,name=settings"`
	// Optional arguments to start nats-server. For example, "-D" to enable debugging output, "-DV" to enable debugging and tracing.
	// Check https://docs.nats.io/ for all the available arguments.
	// +optional
	StartArgs []string `json:"startArgs,omitempty" protobuf:"bytes,9,rep,name=startArgs"`
	// Optional configuration for the streams, consumers and buckets to be created in this JetStream service, if specified, it will be merged with the default configuration in numaflow-controller-config.
	// It accepts a YAML format configuration, it may include 4 sections, "stream", "consumer", "otBucket" and "procBucket".
	// Available fields under "stream" include "retention" (e.g. interest, limits, workerQueue), "maxMsgs", "maxAge" (e.g. 72h), "replicas" (1, 3, 5), "duplicates" (e.g. 5m).
	// Available fields under "consumer" include "ackWait" (e.g. 60s)
	// Available fields under "otBucket" include "maxValueSize", "history", "ttl" (e.g. 72h), "maxBytes", "replicas" (1, 3, 5).
	// Available fields under "procBucket" include "maxValueSize", "history", "ttl" (e.g. 72h), "maxBytes", "replicas" (1, 3, 5).
	// +optional
	BufferConfig *string `json:"bufferConfig,omitempty" protobuf:"bytes,10,opt,name=bufferConfig"`
	// Whether encrypt the data at rest, defaults to false
	// Enabling encryption might impact the performance, see https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/encryption_at_rest for the detail
	// Toggling the value will impact encypting/decrypting existing messages.
	// +optional
	Encryption bool `json:"encryption,omitempty" protobuf:"bytes,11,opt,name=encryption"`
	// Whether enable TLS, defaults to false
	// Enabling TLS might impact the performance
	// +optional
	TLS bool `json:"tls,omitempty" protobuf:"bytes,12,opt,name=tls"`
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
	if j.ReloaderContainerTemplate != nil {
		spec.Template.Spec.Containers[1].Resources = j.ReloaderContainerTemplate.Resources
	}
	if j.MetricsContainerTemplate != nil {
		spec.Template.Spec.Containers[2].Resources = j.MetricsContainerTemplate.Resources
	}
	if j.Persistence != nil {
		spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
			j.Persistence.GetPVCSpec(req.PvcNameIfNeeded),
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
