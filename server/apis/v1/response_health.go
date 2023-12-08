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

// HealthResponse is the response payload for health API.
// It contains the health status of the vertex and data.
// We include the Status, Message and Code for both vertex and data.
type HealthResponse struct {
	VertexHealthStatus  string `json:"vertexHealthStatus"`
	DataHealthStatus    string `json:"dataHealthStatus"`
	VertexHealthMessage string `json:"vertexHealthMessage"`
	DataHealthMessage   string `json:"dataHealthMessage"`
	VertexHealthCode    string `json:"vertexHealthCode"`
	DataHealthCode      string `json:"dataHealthCode"`
}

// NewHealthResponse returns a HealthResponse object for the given status, message and code.
func NewHealthResponse(vertexHealthStatus string, dataHealthStatus string,
	vertexHealthMessage string, dataHealthMessage string, vertexHealthCode string, dataHealthCode string) HealthResponse {
	return HealthResponse{
		VertexHealthStatus:  vertexHealthStatus,
		DataHealthStatus:    dataHealthStatus,
		VertexHealthMessage: vertexHealthMessage,
		DataHealthMessage:   dataHealthMessage,
		VertexHealthCode:    vertexHealthCode,
		DataHealthCode:      dataHealthCode,
	}
}
