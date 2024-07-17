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

// NatsAuth defines how to authenticate the nats access
type NatsAuth struct {
	// Basic auth which contains a username and a password
	// +optional
	Basic *BasicAuth `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`
	// Token auth
	// +optional
	Token *corev1.SecretKeySelector `json:"token,omitempty" protobuf:"bytes,2,opt,name=token"`
	// NKey auth
	// +optional
	NKey *corev1.SecretKeySelector `json:"nkey,omitempty" protobuf:"bytes,3,opt,name=nkey"`
}
