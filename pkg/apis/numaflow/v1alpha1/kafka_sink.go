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

type KafkaSink struct {
	Brokers []string `json:"brokers,omitempty" protobuf:"bytes,1,rep,name=brokers"`
	Topic   string   `json:"topic" protobuf:"bytes,2,opt,name=topic"`
	// TLS user to configure TLS connection for kafka broker
	// TLS.enable=true default for TLS.
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,3,opt,name=tls"`
	// +optional
	Config string `json:"config,omitempty" protobuf:"bytes,4,opt,name=config"`
	// SASL user to configure SASL connection for kafka broker
	// SASL.enable=true default for SASL.
	// +optional
	SASL *SASL `json:"sasl" protobuf:"bytes,5,opt,name=sasl"`
}
