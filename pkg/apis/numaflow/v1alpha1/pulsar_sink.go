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

type PulsarSink struct {
	ServerAddr   string `json:"serverAddr" protobuf:"bytes,1,name=server_addr"`
	Topic        string `json:"topic" protobuf:"bytes,2,name=topic"`
	ProducerName string `json:"producerName" protobuf:"bytes,3,name=producerName"`
	// Auth information
	// +optional
	Auth *PulsarAuth `json:"auth,omitempty" protobuf:"bytes,6,opt,name=auth"`
}
