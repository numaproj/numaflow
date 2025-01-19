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
	// SetKey sets the Kafka key to the keys passed in the Message.
	// When the key is null (default), the record is sent randomly to one of the available partitions of the topic.
	// If a key exists, Kafka hashes the key, and the result is used to map the message to a specific partition. This
	// ensures that messages with the same key end up in the same partition.
	// +optional
	SetKey bool `json:"setKey" protobuf:"varint,3,opt,name=setKey"`
	// TLS user to configure TLS connection for kafka broker
	// TLS.enable=true default for TLS.
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,4,opt,name=tls"`
	// +optional
	Config string `json:"config,omitempty" protobuf:"bytes,5,opt,name=config"`
	// SASL user to configure SASL connection for kafka broker
	// SASL.enable=true default for SASL.
	// +optional
	SASL *SASL `json:"sasl" protobuf:"bytes,6,opt,name=sasl"`
}
