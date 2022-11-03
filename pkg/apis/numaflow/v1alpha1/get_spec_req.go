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
	corev1 "k8s.io/api/core/v1"
)

type GetRedisStatefulSetSpecReq struct {
	ServiceName               string            `protobuf:"bytes,1,opt,name=serviceName"`
	Labels                    map[string]string `protobuf:"bytes,2,rep,name=labels"`
	RedisImage                string            `protobuf:"bytes,3,opt,name=redisImage"`
	SentinelImage             string            `protobuf:"bytes,4,opt,name=sentinelImage"`
	MetricsExporterImage      string            `protobuf:"bytes,5,opt,name=metricsExporterImage"`
	InitContainerImage        string            `protobuf:"bytes,6,opt,name=initContainerImage"`
	RedisContainerPort        int32             `protobuf:"bytes,7,opt,name=redisContainerPort"`
	SentinelContainerPort     int32             `protobuf:"bytes,8,opt,name=sentinelContainerPort"`
	RedisMetricsContainerPort int32             `protobuf:"bytes,9,opt,name=redisMetricsContainerPort"`
	CredentialSecretName      string            `protobuf:"bytes,10,opt,name=credentialSecretName"`
	TLSEnabled                bool              `protobuf:"bytes,11,opt,name=tlsEnabled"`
	PvcNameIfNeeded           string            `protobuf:"bytes,12,opt,name=pvcNameIfNeeded"`
	ConfConfigMapName         string            `protobuf:"bytes,13,opt,name=confConfigMapName"`
	ScriptsConfigMapName      string            `protobuf:"bytes,14,opt,name=scriptsConfigMapName"`
	HealthConfigMapName       string            `protobuf:"bytes,15,opt,name=healthConfigMapName"`
}

type GetRedisServiceSpecReq struct {
	Labels                map[string]string `protobuf:"bytes,1,rep,name=labels"`
	RedisContainerPort    int32             `protobuf:"bytes,2,opt,name=redisContainerPort"`
	SentinelContainerPort int32             `protobuf:"bytes,3,opt,name=sentinelContainerPort"`
}

type GetVertexPodSpecReq struct {
	ISBSvcType ISBSvcType        `protobuf:"bytes,1,opt,name=isbSvcType"`
	Image      string            `protobuf:"bytes,2,opt,name=image"`
	PullPolicy corev1.PullPolicy `protobuf:"bytes,3,opt,name=pullPolicy,casttype=k8s.io/api/core/v1.PullPolicy"`
	Env        []corev1.EnvVar   `protobuf:"bytes,4,rep,name=env"`
}

type GetDaemonDeploymentReq struct {
	ISBSvcType ISBSvcType        `protobuf:"bytes,1,opt,name=isbSvcType"`
	Image      string            `protobuf:"bytes,2,opt,name=image"`
	PullPolicy corev1.PullPolicy `protobuf:"bytes,3,opt,name=pullPolicy,casttype=k8s.io/api/core/v1.PullPolicy"`
	Env        []corev1.EnvVar   `protobuf:"bytes,4,rep,name=env"`
}

type GetJetStreamStatefulSetSpecReq struct {
	ServiceName                string            `protobuf:"bytes,1,rep,name=serviceName"`
	Labels                     map[string]string `protobuf:"bytes,2,rep,name=labels"`
	NatsImage                  string            `protobuf:"bytes,3,opt,name=natsImage"`
	MetricsExporterImage       string            `protobuf:"bytes,4,opt,name=metricsExporterImage"`
	ConfigReloaderImage        string            `protobuf:"bytes,5,opt,name=configReloaderImage"`
	ClusterPort                int32             `protobuf:"bytes,6,opt,name=clusterPort"`
	ClientPort                 int32             `protobuf:"bytes,7,opt,name=clientPort"`
	MonitorPort                int32             `protobuf:"bytes,8,opt,name=monitorPort"`
	MetricsPort                int32             `protobuf:"bytes,9,opt,name=metricsPort"`
	ServerAuthSecretName       string            `protobuf:"bytes,10,opt,name=serverAuthSecretName"`
	ServerEncryptionSecretName string            `protobuf:"bytes,11,opt,name=serverEncryptionSecretName"`
	ConfigMapName              string            `protobuf:"bytes,12,opt,name=configMapName"`
	PvcNameIfNeeded            string            `protobuf:"bytes,13,opt,name=pvcNameIfNeeded"`
	StartCommand               string            `protobuf:"bytes,14,opt,name=startCommand"`
}

type GetJetStreamServiceSpecReq struct {
	Labels      map[string]string `protobuf:"bytes,1,rep,name=labels"`
	ClusterPort int32             `protobuf:"bytes,2,opt,name=clusterPort"`
	ClientPort  int32             `protobuf:"bytes,3,opt,name=clientPort"`
	MonitorPort int32             `protobuf:"bytes,4,opt,name=monitorPort"`
	MetricsPort int32             `protobuf:"bytes,5,opt,name=metricsPort"`
}
