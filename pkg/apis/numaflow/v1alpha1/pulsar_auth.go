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

import corev1 "k8s.io/api/core/v1"

// PulsarAuth defines how to authenticate with Pulsar
type PulsarAuth struct {
	// JWT Token auth
	// +optional
	Token *corev1.SecretKeySelector `json:"token,omitempty" protobuf:"bytes,1,opt,name=token"`
	// Authentication using HTTP basic https://pulsar.apache.org/docs/4.0.x/security-basic-auth/
	// +optional
	BasicAuth *PulsarBasicAuth `json:"basicAuth,omitempty" protobuf:"bytes,2,opt,name=basicAuth"`
}

type PulsarBasicAuth struct {
	// +optional
	Username *corev1.SecretKeySelector `json:"username,omitempty" protobuf:"bytes,1,opt,name=username"`
	// +optional
	Password *corev1.SecretKeySelector `json:"password,omitempty" protobuf:"bytes,2,opt,name=password"`
}
