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

// ActiveStatus contains the number of objects in healthy, warning, and critical status.
type ActiveStatus struct {
	Healthy  int `json:"Healthy"`
	Warning  int `json:"Warning"`
	Critical int `json:"Critical"`
}

func (as *ActiveStatus) increment(status string) {
	if status == PipelineStatusHealthy {
		as.Healthy++
	} else if status == PipelineStatusWarning {
		as.Warning++
	} else if status == PipelineStatusCritical {
		as.Critical++
	}
}

// PipelineSummary summarizes the number of active and inactive pipelines.
type PipelineSummary struct {
	Active   ActiveStatus `json:"active"`
	Inactive int          `json:"inactive"`
}

// IsbServiceSummary summarizes the number of active and inactive ISB Service.
type IsbServiceSummary struct {
	Active   ActiveStatus `json:"active"`
	Inactive int          `json:"inactive"`
}

// ClusterSummaryResponse is a list of ClusterSummary
// of all the namespaces in a cluster wrapped in a list.
type ClusterSummaryResponse []ClusterSummary

// ClusterSummary summarizes information for a given namespace.
type ClusterSummary struct {
	Namespace         string            `json:"namespace"`
	PipelineSummary   PipelineSummary   `json:"pipelineSummary"`
	IsbServiceSummary IsbServiceSummary `json:"isbServiceSummary"`
}

// NewClusterSummary creates a new ClusterSummary object with the given specifications.
func NewClusterSummary(namespace string, pipelineSummary PipelineSummary,
	isbSummary IsbServiceSummary) ClusterSummary {
	return ClusterSummary{
		Namespace:         namespace,
		PipelineSummary:   pipelineSummary,
		IsbServiceSummary: isbSummary,
	}
}
