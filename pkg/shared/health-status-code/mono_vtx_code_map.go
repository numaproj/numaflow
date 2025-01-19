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

var monoVtxResourceMap = map[string]*HealthCodeInfo{
	"M1": newHealthCodeInfo(
		"Mono Vertex is healthy",
		"Healthy",
	),
	"M2": newHealthCodeInfo(
		"Mono Vertex is in a critical state",
		"Critical",
	),
	"M3": newHealthCodeInfo(
		"Mono Vertex is in a warning state",
		"Warning",
	),
	"M4": newHealthCodeInfo(
		"Mono Vertex is in an unknown state",
		"Critical",
	),
	"M5": newHealthCodeInfo(
		"Mono Vertex is in a paused state",
		"Warning",
	),
}

// monoVtxDataflowHealthMap is used to maintain status codes for dataflow level health
// Each map entry is a map of status code as the key to the status message and the criticality of the status.
// Status codes are in incremental like
// 1. D1
// 2. D2
// 3. D3
// The criticality is used to determine the overall status of the MonoVertex
// Criticality can be one of the following:
// 1. Critical: The MonoVertex is in a critical state
// 2. Warning: The MonoVertex is in a warning state
// 3. Healthy: The MonoVertex is healthy

var _ = map[string]*HealthCodeInfo{
	"D1": newHealthCodeInfo(
		"Dataflow is healthy",
		"Healthy",
	),
	"D2": newHealthCodeInfo(
		"Dataflow in warning state",
		"Warning",
	),
	"D3": newHealthCodeInfo(
		"Dataflow in critical state",
		"Critical",
	),
	"D4": newHealthCodeInfo(
		"Dataflow in unknown state",
		"Critical",
	),
}
