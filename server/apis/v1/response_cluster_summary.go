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

import dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

// ActiveStatus contains the number of objects in healthy, warning, and critical status.
type ActiveStatus struct {
	Healthy  int `json:"Healthy"`
	Warning  int `json:"Warning"`
	Critical int `json:"Critical"`
}

func (as *ActiveStatus) isEmpty() bool {
	return as.Healthy == 0 && as.Warning == 0 && as.Critical == 0
}

func (as *ActiveStatus) increment(status string) {
	if status == dfv1.PipelineStatusHealthy {
		as.Healthy++
	} else if status == dfv1.PipelineStatusWarning {
		as.Warning++
	} else if status == dfv1.PipelineStatusCritical {
		as.Critical++
	}
}

// PipelineSummary summarizes the number of active and inactive pipelines.
type PipelineSummary struct {
	Active   ActiveStatus `json:"active"`
	Inactive int          `json:"inactive"`
}

func (ps *PipelineSummary) hasPipeline() bool {
	return ps.Inactive > 0 || !ps.Active.isEmpty()
}

// IsbServiceSummary summarizes the number of active and inactive ISB Service.
type IsbServiceSummary struct {
	Active   ActiveStatus `json:"active"`
	Inactive int          `json:"inactive"`
}

func (is *IsbServiceSummary) hasIsbService() bool {
	return is.Inactive > 0 || !is.Active.isEmpty()
}

// ClusterSummaryResponse is a list of NamespaceSummary
// of all the namespaces in a cluster wrapped in a list.
type ClusterSummaryResponse []NamespaceSummary

// NamespaceSummary summarizes information for a given namespace.
type NamespaceSummary struct {
	// IsEmpty indicates whether there are numaflow resources in the namespace.
	// resources include pipelines and ISB services.
	IsEmpty bool `json:"isEmpty"`
	// Namespace is the name of the namespace.
	Namespace         string            `json:"namespace"`
	PipelineSummary   PipelineSummary   `json:"pipelineSummary"`
	IsbServiceSummary IsbServiceSummary `json:"isbServiceSummary"`
}

// NewNamespaceSummary creates a new NamespaceSummary object with the given specifications.
func NewNamespaceSummary(namespace string, pipelineSummary PipelineSummary,
	isbSummary IsbServiceSummary) NamespaceSummary {
	return NamespaceSummary{
		IsEmpty:           !(pipelineSummary.hasPipeline() || isbSummary.hasIsbService()),
		Namespace:         namespace,
		PipelineSummary:   pipelineSummary,
		IsbServiceSummary: isbSummary,
	}
}
