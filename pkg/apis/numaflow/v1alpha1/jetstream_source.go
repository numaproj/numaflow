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

type JetStreamSource struct {
	// URL to connect to NATS cluster, multiple urls could be separated by comma.
	URL string `json:"url" protobuf:"bytes,1,opt,name=url"`
	// Stream represents the name of the stream.
	Stream string `json:"stream" protobuf:"bytes,2,opt,name=stream"`
	// Consumer represents the name of the consumer of the stream
	// If not specified, a consumer with name `numaflow-pipeline_name-vertex_name-stream_name` will be created.
	// If a consumer name is specified, a consumer with that name will be created if it doesn't exist on the stream.
	// +optional
	Consumer string `json:"consumer" protobuf:"bytes,3,opt,name=consumer"`
	// The point in the stream from which to receive messages.
	// https://docs.nats.io/nats-concepts/jetstream/consumers#deliverpolicy
	// Valid options are: "all", "new", "last", "last_per_subject", "by_start_sequence 42", "by_start_time 1753428483000".
	// The second value to "by_start_time" is unix epoch time in milliseconds.
	// +optional
	DeliverPolicy string `json:"deliver_policy" protobuf:"bytes,4,opt,name=deliver_policy"`
	// A set of subjects that overlap with the subjects bound to the stream to filter delivery to subscribers.
	// https://docs.nats.io/nats-concepts/jetstream/consumers#filtesubjects
	// +optional
	FilterSubjects []string `json:"filter_subjects" protobuf:"bytes,5,opt,name=filter_subjects"`
	// TLS configuration for the nats client.
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,6,opt,name=tls"`
	// Auth information
	// +optional
	Auth *NatsAuth `json:"auth,omitempty" protobuf:"bytes,7,opt,name=auth"`
}
