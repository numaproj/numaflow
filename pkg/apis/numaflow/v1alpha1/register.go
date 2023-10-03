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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// SchemeGroupVersion is group version used to register these objects.
	SchemeGroupVersion = schema.GroupVersion{Group: "numaflow.numaproj.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme

	ISBGroupVersionKind          = SchemeGroupVersion.WithKind("InterStepBufferService")
	ISBGroupVersionResource      = SchemeGroupVersion.WithResource("interstepbufferservices")
	PipelineGroupVersionKind     = SchemeGroupVersion.WithKind("Pipeline")
	PipelineGroupVersionResource = SchemeGroupVersion.WithResource("pipelines")
	VertexGroupVersionKind       = SchemeGroupVersion.WithKind("Vertex")
	VertexGroupVersionResource   = SchemeGroupVersion.WithResource("vertices")
)

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&InterStepBufferService{},
		&InterStepBufferServiceList{},
		&Pipeline{},
		&PipelineList{},
		&Vertex{},
		&VertexList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
