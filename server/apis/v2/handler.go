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

package v2

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/apis/v2/generated"
	"github.com/numaproj/numaflow/server/application/capabilities"
)

// CapabilitiesService provides transport-independent API v2 discovery data.
type CapabilitiesService interface {
	GetCapabilities() capabilities.Capabilities
}

// Handler adapts API v2 application services to HTTP handlers.
type Handler struct {
	service CapabilitiesService
}

// NewHandler builds the API v2 HTTP adapter around the capabilities application service.
func NewHandler(service CapabilitiesService) (*Handler, error) {
	if service == nil {
		return nil, fmt.Errorf("capabilities service is required")
	}
	return &Handler{service: service}, nil
}

var _ generated.ServerInterface = (*Handler)(nil)

// GetCapabilities returns the API v2 discovery document for the current server.
func (h *Handler) GetCapabilities(c *gin.Context) {
	c.JSON(http.StatusOK, toCapabilities(h.service.GetCapabilities()))
}

// toCapabilities translates transport-independent application types into the
// OpenAPI-generated response type.
func toCapabilities(value capabilities.Capabilities) generated.Capabilities {
	return generated.Capabilities{
		ApiVersion: value.APIVersion,
		Operations: value.Operations,
		Limits: generated.ApiLimits{
			DefaultPageSize:     value.Limits.DefaultPageSize,
			MaximumPageSize:     value.Limits.MaximumPageSize,
			MaximumLogLines:     value.Limits.MaximumLogLines,
			MaximumMetricPoints: value.Limits.MaximumMetricPoints,
		},
	}
}
