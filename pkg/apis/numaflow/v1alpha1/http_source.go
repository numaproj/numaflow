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

type HTTPSource struct {
	// +optional
	Auth *Authorization `json:"auth" protobuf:"bytes,1,opt,name=auth"`
	// Whether to create a ClusterIP Service
	// +optional
	Service bool `json:"service" protobuf:"bytes,2,opt,name=service"`
	// Ports to listen on. HTTPS always runs on port 8443 by default.
	// To enable plain HTTP, set ports.http explicitly.
	// +optional
	Ports *Ports `json:"ports,omitempty" protobuf:"bytes,3,opt,name=ports"`
}

// GetHTTPSPort returns the configured HTTPS port, or the default VertexHTTPSPort (8443).
func (h HTTPSource) GetHTTPSPort() int32 {
	if h.Ports != nil && h.Ports.HTTPS != nil {
		return *h.Ports.HTTPS
	}
	return VertexHTTPSPort
}

// GetHTTPPort returns the configured HTTP port, or the default VertexHTTPPort (8090).
func (h HTTPSource) GetHTTPPort() int32 {
	if h.Ports != nil && h.Ports.HTTP != nil {
		return *h.Ports.HTTP
	}
	return VertexHTTPPort
}

// IsHTTPConfigured returns true if plain HTTP is explicitly enabled by the user.
func (h HTTPSource) IsHTTPConfigured() bool {
	return h.Ports != nil && h.Ports.HTTP != nil
}

type Authorization struct {
	// A secret selector which contains bearer token
	// To use this, the client needs to add "Authorization: Bearer <token>" in the header
	// +optional
	Token *corev1.SecretKeySelector `json:"token" protobuf:"bytes,1,opt,name=token"`
}
