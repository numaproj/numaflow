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

// Pipelines is a list of pipelines
type Pipelines []PipelineInfo

type PipelineInfo struct {
	Name string `json:"name"`
	// Status shows whether the pipeline is healthy, warning, critical or inactive.
	Status string `json:"status"`
	// Lag shows the pipeline lag.
	Lag *int64 `json:"lag,omitempty"`
	// Pipeline contains the detailed pipeline spec.
	Pipeline v1alpha1.Pipeline `json:"pipeline"`
}

// NewPipelineInfo creates a new PipelineInfo object with the given status
func NewPipelineInfo(status string, lag *int64, pl *v1alpha1.Pipeline) PipelineInfo {
	return PipelineInfo{
		Name:     pl.Name,
		Status:   status,
		Lag:      lag,
		Pipeline: *pl,
	}
}
