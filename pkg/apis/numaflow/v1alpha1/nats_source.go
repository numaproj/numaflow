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

type NatsSource struct {
	// URL to connect to NATS cluster, multiple urls could be separated by comma.
	URL string `json:"url" protobuf:"bytes,1,opt,name=url"`
	// Subject holds the name of the subject onto which messages are published.
	Subject string `json:"subject" protobuf:"bytes,2,opt,name=subject"`
	// Queue is used for queue subscription.
	Queue string `json:"queue" protobuf:"bytes,3,opt,name=queue"`
	// TLS configuration for the nats client.
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,4,opt,name=tls"`
	// Auth information
	// +optional
	Auth *NatsAuth `json:"auth,omitempty" protobuf:"bytes,5,opt,name=auth"`
}
