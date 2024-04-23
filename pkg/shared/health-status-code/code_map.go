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

package health_status_code

// HealthCodeInfo is used to maintain status codes for vertex level health
type HealthCodeInfo struct {
	Status      string
	Criticality string
}

// newHealthCodeInfo is used to create a new HealthCodeInfo object
func newHealthCodeInfo(status string, criticality string) *HealthCodeInfo {
	return &HealthCodeInfo{
		Status:      status,
		Criticality: criticality,
	}
}

// VertexHealthMap is used to maintain status codes for vertex level health
// Each map entry is a map of status code as the key to the status message and the criticality of the status.
// Status codes are in incremental like
// 1. V1
// 2. V2
// 3. V3
// The criticality is used to determine the overall status of the pipeline
// Criticality can be one of the following:
// 1. Critical: The pipeline is in a critical state
// 2. Warning: The pipeline is in a warning state
// 3. Healthy: The pipeline is healthy
// 4. Unknown: The pipeline is in an unknown state
var vertexHealthMap = map[string]*HealthCodeInfo{
	"V1": newHealthCodeInfo(
		"All pods are running",
		"Healthy",
	),
	"V2": newHealthCodeInfo(
		"Vertex is not in running state",
		"Critical",
	),
	"V3": newHealthCodeInfo(
		"Vertex is in running but containers are not in running state",
		"Warning",
	),
	"V4": newHealthCodeInfo(
		"All vertices healthy in the pipeline",
		"Healthy",
	),
	"V5": newHealthCodeInfo(
		"One or more vertices are unhealthy in the pipeline",
		"Warning",
	),
	"V6": newHealthCodeInfo(
		"Pipeline is in an unknown state",
		"Critical",
	),
	"V7": newHealthCodeInfo(
		"Pipeline is paused",
		"Healthy",
	),
	"V8": newHealthCodeInfo(
		"Pipeline is killed",
		"Healthy",
	),
	"V9": newHealthCodeInfo(
		"Vertex not able to get the desired number of replicas",
		"Warning",
	),
}

// DataflowHealthCodeMap is used to maintain status codes for dataflow level health
// Each map entry is a map of status code as the key to the status message and the criticality of the status.
// Status codes are in incremental like
// 1. D1
// 2. D2
// 3. D3
// The criticality is used to determine the overall status of the pipeline
// Criticality can be one of the following:
// 1. Critical: The pipeline is in a critical state
// 2. Warning: The pipeline is in a warning state
// 3. Healthy: The pipeline is healthy

var dataflowHealthMap = map[string]*HealthCodeInfo{
	"D1": newHealthCodeInfo(
		"Dataflow is healthy",
		"Healthy",
	),
	"D2": newHealthCodeInfo(
		"Dataflow in warning state for one or more vertices",
		"Warning",
	),
	"D3": newHealthCodeInfo(
		"Dataflow in critical state for one or more vertices",
		"Critical",
	),
	"D4": newHealthCodeInfo(
		"Dataflow in unknown state",
		"Critical",
	),
}

// getHealthCodeInfo is used to get the status code information for a given status code
func getHealthCodeInfo(code string) *HealthCodeInfo {
	if status, ok := vertexHealthMap[code]; ok {
		return status
	}
	if status, ok := dataflowHealthMap[code]; ok {
		return status
	}
	return nil
}
