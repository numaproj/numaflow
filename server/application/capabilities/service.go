/*
Copyright 2026 The Numaproj Authors.

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

package capabilities

// Service provides the API v2 discovery document. It does not own UI preferences.
type Service struct{}

// NewService creates the API v2 capability service.
func NewService() *Service {
	return &Service{}
}

// GetCapabilities describes API v2 operations and limits available from this server.
func (s *Service) GetCapabilities() Capabilities {
	return Capabilities{
		APIVersion: "v2",
		// operationIds must match OpenAPI and mounted handlers (see server/apis/v2).
		Operations: []string{"getCapabilities", "getPipelineVertexSummary"},
		Limits: Limits{
			DefaultPageSize:     50,
			MaximumPageSize:     200,
			MaximumLogLines:     1000,
			MaximumMetricPoints: 2000,
		},
	}
}
