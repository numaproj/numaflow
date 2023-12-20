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
// We include the Status, Message and Code for both resources and data.
type HealthResponse struct {
	ResourceHealthStatus  string `json:"resourceHealthStatus"`
	DataHealthStatus      string `json:"dataHealthStatus"`
	ResourceHealthMessage string `json:"resourceHealthMessage"`
	DataHealthMessage     string `json:"dataHealthMessage"`
	ResourceHealthCode    string `json:"resourceHealthCode"`
	DataHealthCode        string `json:"dataHealthCode"`
}

// NewHealthResponse returns a HealthResponse object for the given status, message and code.
func NewHealthResponse(resourceHealthStatus string, dataHealthStatus string,
	resourceHealthMessage string, dataHealthMessage string, resourceHealthCode string,
	dataHealthCode string) HealthResponse {
	return HealthResponse{
		ResourceHealthStatus:  resourceHealthStatus,
		DataHealthStatus:      dataHealthStatus,
		ResourceHealthMessage: resourceHealthMessage,
		DataHealthMessage:     dataHealthMessage,
		ResourceHealthCode:    resourceHealthCode,
		DataHealthCode:        dataHealthCode,
	}
}
