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

package v1

import "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

// MonoVertices is a list of mono vertices
type MonoVertices []MonoVertexInfo

type MonoVertexInfo struct {
	Name string `json:"name"`
	// Status shows whether the mono vertex is healthy, warning, critical or inactive.
	Status string `json:"status"`
	// Lag shows the mono vertex lag.
	Lag *int64 `json:"lag,omitempty"`
	// MonoVertex contains the detailed mono vertex spec.
	MonoVertex v1alpha1.MonoVertex `json:"monoVertex"`
}

// NewMonoVertexInfo creates a new MonoVertexInfo object with the given status and lag
func NewMonoVertexInfo(status string, lag *int64, mvt *v1alpha1.MonoVertex) MonoVertexInfo {
	return MonoVertexInfo{
		Name:       mvt.Name,
		Status:     status,
		Lag:        lag,
		MonoVertex: *mvt,
	}
}
