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

type VertexTemplate struct {
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,1,opt,name=abstractPodTemplate"`
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,2,opt,name=containerTemplate"`
	// +optional
	InitContainerTemplate *ContainerTemplate `json:"initContainerTemplate,omitempty" protobuf:"bytes,3,opt,name=initContainerTemplate"`
}

// ApplyToAbstractVertex updates the AbstractVertex with the values in the VertexTemplate
func (vt *VertexTemplate) ApplyToAbstractVertex(av *AbstractVertex) error {
	vtTemplate := Templates{
		VertexTemplate: vt,
	}
	avTemplate := Templates{
		VertexTemplate: &VertexTemplate{
			AbstractPodTemplate:   av.AbstractPodTemplate,
			ContainerTemplate:     av.ContainerTemplate,
			InitContainerTemplate: av.InitContainerTemplate,
		},
	}
	err := avTemplate.UpdateWithDefaultsFrom(&vtTemplate)
	if err != nil {
		return err
	}
	av.AbstractPodTemplate = avTemplate.VertexTemplate.AbstractPodTemplate
	av.ContainerTemplate = avTemplate.VertexTemplate.ContainerTemplate
	av.InitContainerTemplate = avTemplate.VertexTemplate.InitContainerTemplate
	return nil
}
