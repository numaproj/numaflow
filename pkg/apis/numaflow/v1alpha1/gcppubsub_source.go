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

type GCPPubSubSource struct {
	// +optional
	ProjectID string `json:"projectID" protobuf:"bytes,1,opt,name=projectID"`
	// +optional
	TopicProjectID string `json:"topicProjectID" protobuf:"bytes,2,opt,name=topicProjectID"`
	// +optional
	Topic string `json:"topic" protobuf:"bytes,3,opt,name=topic"`
	// +optional
	SubscriptionID string `json:"subscriptionID" protobuf:"bytes,4,opt,name=subscriptionID"`
	// +optional
	CredentialSecret *corev1.SecretKeySelector `json:"credentialSecret,omitempty" protobuf:"bytes,5,opt,name=credentialSecret"`
}
