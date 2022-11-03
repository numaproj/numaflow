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

type Edge struct {
	From string `json:"from" protobuf:"bytes,1,opt,name=from"`
	To   string `json:"to" protobuf:"bytes,2,opt,name=to"`
	// Conditional forwarding, only allowed when "From" is a Sink or UDF.
	// +optional
	Conditions *ForwardConditions `json:"conditions" protobuf:"bytes,3,opt,name=conditions"`
	// Limits define the limitations such as buffer read batch size for the edge, will override pipeline level settings.
	// +optional
	Limits *EdgeLimits `json:"limits,omitempty" protobuf:"bytes,4,opt,name=limits"`
	// Parallelism is only effective when the "to" vertex is a reduce vertex,
	// if it's provided, the default value is set to "1".
	// Parallelism is ignored when the "to" vertex is not a reduce vertex.
	// +optional
	Parallelism *int32 `json:"parallelism" protobuf:"bytes,5,opt,name=parallelism"`
}

type ForwardConditions struct {
	KeyIn []string `json:"keyIn" protobuf:"bytes,1,rep,name=keyIn"`
}

type EdgeLimits struct {
	// BufferMaxLength is used to define the max length of a buffer.
	// It overrides the settings from pipeline limits.
	// +optional
	BufferMaxLength *uint64 `json:"bufferMaxLength,omitempty" protobuf:"varint,1,opt,name=bufferMaxLength"`
	// BufferUsageLimit is used to define the percentage of the buffer usage limit, a valid value should be less than 100, for example, 85.
	// It overrides the settings from pipeline limits.
	// +optional
	BufferUsageLimit *uint32 `json:"bufferUsageLimit,omitempty" protobuf:"varint,2,opt,name=bufferUsageLimit"`
}
