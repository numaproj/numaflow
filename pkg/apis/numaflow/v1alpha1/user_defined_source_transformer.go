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

type UDTransformer struct {
	// +optional
	Container *Container `json:"container" protobuf:"bytes,1,opt,name=container"`
	// +optional
	Builtin *Transformer `json:"builtin" protobuf:"bytes,2,opt,name=builtin"`
}

type Transformer struct {
	// +kubebuilder:validation:Enum=eventTimeExtractor;filter;timeExtractionFilter
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// +optional
	Args []string `json:"args,omitempty" protobuf:"bytes,2,rep,name=args"`
	// +optional
	KWArgs map[string]string `json:"kwargs,omitempty" protobuf:"bytes,3,rep,name=kwargs"`
}
