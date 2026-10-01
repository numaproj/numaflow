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
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/numaproj/numaflow/server/apis/v2/generated"
	"github.com/numaproj/numaflow/server/application/capabilities"
	"github.com/numaproj/numaflow/server/application/observability"
)

// CapabilitiesService provides transport-independent API v2 discovery data.
type CapabilitiesService interface {
	GetCapabilities() capabilities.Capabilities
}

// PipelineVertexSummaryService serves Kubernetes-backed pipeline vertex observability reads.
type PipelineVertexSummaryService interface {
	GetPipelineVertexSummary(ctx context.Context, namespace, pipeline, vertex string) (observability.Result[observability.VertexSummary], error)
	GetPipelineVertexStatus(ctx context.Context, namespace, pipeline, vertex string) (observability.Result[observability.VertexStatus], error)
}

// Handler adapts API v2 application services to HTTP handlers.
type Handler struct {
	capabilitiesService CapabilitiesService
	summaryService      PipelineVertexSummaryService
}

// NewHandler builds the API v2 HTTP adapter from discovery and observability services.
func NewHandler(capabilitiesService CapabilitiesService, summaryService PipelineVertexSummaryService) (*Handler, error) {
	if capabilitiesService == nil {
		return nil, fmt.Errorf("capabilities service is required")
	}
	if summaryService == nil {
		return nil, fmt.Errorf("pipeline vertex summary service is required")
	}
	return &Handler{capabilitiesService: capabilitiesService, summaryService: summaryService}, nil
}

var _ generated.ServerInterface = (*Handler)(nil)

// GetCapabilities returns the API v2 discovery document for the current server.
func (h *Handler) GetCapabilities(c *gin.Context) {
	c.JSON(http.StatusOK, toCapabilities(h.capabilitiesService.GetCapabilities()))
}

// GetPipelineVertexSummary returns a compact controller-backed Vertex projection.
func (h *Handler) GetPipelineVertexSummary(c *gin.Context, namespace generated.Namespace, pipeline generated.Pipeline, vertex generated.Vertex, params generated.GetPipelineVertexSummaryParams) {
	if !validateNames(c, nameField{"namespace", namespace}, nameField{"pipeline", pipeline}, nameField{"vertex", vertex}) {
		return
	}
	result, err := h.summaryService.GetPipelineVertexSummary(c.Request.Context(), namespace, pipeline, vertex)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	writeVersioned(c, result.ResourceVersion, params.IfNoneMatch, toVertexSummary(result.Value))
}

// GetPipelineVertexStatus returns bounded controller detail for one pipeline vertex.
func (h *Handler) GetPipelineVertexStatus(c *gin.Context, namespace generated.Namespace, pipeline generated.Pipeline, vertex generated.Vertex, params generated.GetPipelineVertexStatusParams) {
	if !validateNames(c, nameField{"namespace", namespace}, nameField{"pipeline", pipeline}, nameField{"vertex", vertex}) {
		return
	}
	result, err := h.summaryService.GetPipelineVertexStatus(c.Request.Context(), namespace, pipeline, vertex)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	writeVersioned(c, result.ResourceVersion, params.IfNoneMatch, toVertexStatus(result.Value))
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

type nameField struct {
	name  string
	value string
}

// validateNames rejects path segments that are not valid DNS-1123 subdomain labels.
func validateNames(c *gin.Context, fields ...nameField) bool {
	violations := make([]generated.Violation, 0)
	for _, field := range fields {
		for _, message := range validation.IsDNS1123Subdomain(field.value) {
			violations = append(violations, generated.Violation{
				Field:   field.name,
				Reason:  "invalid_kubernetes_name",
				Message: message,
			})
		}
	}
	if len(violations) == 0 {
		return true
	}
	WriteProblem(c, http.StatusUnprocessableEntity, "validation_failed", "Request validation failed", "One or more target names are invalid", violations)
	return false
}

// writeServiceError maps Kubernetes read failures to stable Problem codes without leaking provider text.
func writeServiceError(c *gin.Context, err error) {
	if apierrors.IsNotFound(err) {
		WriteProblem(c, http.StatusNotFound, "target_not_found", "Target not found", "The requested pipeline vertex does not exist", nil)
		return
	}
	WriteProblem(c, http.StatusInternalServerError, "target_read_failed", "Target read failed", "Failed to read the requested pipeline vertex", nil)
}

// writeVersioned sets an ETag from the CR resourceVersion and honors If-None-Match with 304.
func writeVersioned[T any](c *gin.Context, resourceVersion string, ifNoneMatch *string, value T) {
	etag := fmt.Sprintf("%q", resourceVersion)
	c.Header("ETag", etag)
	c.Header("Cache-Control", "private, no-cache")
	if ifNoneMatch != nil && strings.TrimSpace(*ifNoneMatch) == etag {
		c.Status(http.StatusNotModified)
		return
	}
	c.JSON(http.StatusOK, value)
}

func toVertexSummary(value observability.VertexSummary) generated.VertexSummary {
	return generated.VertexSummary{
		Ref: generated.TargetRef{
			Kind:      generated.PipelineVertex,
			Namespace: value.Ref.Namespace,
			Pipeline:  value.Ref.Pipeline,
			Name:      value.Ref.Name,
			Uid:       value.Ref.UID,
		},
		VertexType:         generated.VertexType(value.VertexType),
		Phase:              value.Phase,
		DesiredPhase:       value.DesiredPhase,
		Health:             toHealth(value.Health),
		Generation:         value.Generation,
		ObservedGeneration: value.ObservedGeneration,
		CreatedAt:          value.CreatedAt,
		ObservedAt:         value.ObservedAt,
		LastScaledAt:       value.LastScaledAt,
		Capabilities:       value.Capabilities,
		TruncatedFields:    optionalStrings(value.TruncatedFields),
	}
}

func toHealth(value observability.Health) generated.Health {
	return generated.Health{
		State:   generated.HealthState(value.State),
		Reason:  optionalString(value.Reason),
		Message: optionalString(value.Message),
	}
}

func toVertexStatus(value observability.VertexStatus) generated.VertexStatus {
	conditions := make([]generated.Condition, 0, len(value.Conditions))
	for _, condition := range value.Conditions {
		conditions = append(conditions, generated.Condition{
			Type:               condition.Type,
			Status:             generated.ConditionStatus(condition.Status),
			Reason:             condition.Reason,
			Message:            optionalString(condition.Message),
			ObservedGeneration: condition.ObservedGeneration,
			LastTransitionTime: condition.LastTransitionTime,
		})
	}
	return generated.VertexStatus{
		Ref: generated.TargetRef{
			Kind:      generated.PipelineVertex,
			Namespace: value.Ref.Namespace,
			Pipeline:  value.Ref.Pipeline,
			Name:      value.Ref.Name,
			Uid:       value.Ref.UID,
		},
		Phase:        value.Phase,
		DesiredPhase: value.DesiredPhase,
		Reason:       optionalString(value.Reason),
		Message:      optionalString(value.Message),
		Replicas: generated.ReplicaStatus{
			Current:      value.Replicas.Current,
			Desired:      value.Replicas.Desired,
			Ready:        value.Replicas.Ready,
			Updated:      value.Replicas.Updated,
			UpdatedReady: value.Replicas.UpdatedReady,
		},
		Conditions:         conditions,
		Generation:         value.Generation,
		ObservedGeneration: value.ObservedGeneration,
		ObservedAt:         value.ObservedAt,
		TruncatedFields:    optionalStrings(value.TruncatedFields),
	}
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func optionalStrings(value []string) *[]string {
	if len(value) == 0 {
		return nil
	}
	return &value
}
