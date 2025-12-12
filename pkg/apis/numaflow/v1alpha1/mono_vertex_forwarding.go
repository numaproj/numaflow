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

type MonoVertexForwarding struct {
	// +optional
	SourceForwarding *MonoVertexSinkerCondition `json:"fromSource" protobuf:"bytes,1,opt,name=source"`
	// +optional
	MapForwarding *MonoVertexSinkerCondition `json:"fromMap" protobuf:"bytes,2,opt,name=map"`
}

type MonoVertexSinkerCondition struct {
	// +optional
	Sink *ForwardConditions `json:"sink" protobuf:"bytes,1,opt,name=sink"`
	// +optional
	Fallback *ForwardConditions `json:"fallback" protobuf:"bytes,2,opt,name=fallback"`
	// +optional
	OnSuccess *ForwardConditions `json:"onSuccess" protobuf:"bytes,3,opt,name=onSuccess"`
}
