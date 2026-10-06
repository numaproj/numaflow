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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/numaproj/numaflow/server/apis/v2/generated"
	"github.com/numaproj/numaflow/server/application/capabilities"
	"github.com/numaproj/numaflow/server/application/observability"
)

func TestGetCapabilities(t *testing.T) {
	service := &fakeCapabilitiesService{capabilities: capabilities.Capabilities{
		APIVersion: "v2",
		Operations: []string{"getCapabilities", "getPipelineVertexSummary", "getPipelineVertexStatus", "getMonoVertexSummary", "getMonoVertexStatus"},
		Limits: capabilities.Limits{
			DefaultPageSize:     11,
			MaximumPageSize:     22,
			MaximumLogLines:     33,
			MaximumMetricPoints: 44,
		},
	}}
	router := testRouter(t, service, &fakeObservabilityService{})
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/capabilities", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	var response generated.Capabilities
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, generated.Capabilities{
		ApiVersion: "v2",
		Operations: []string{"getCapabilities", "getPipelineVertexSummary", "getPipelineVertexStatus", "getMonoVertexSummary", "getMonoVertexStatus"},
		Limits: generated.ApiLimits{
			DefaultPageSize:     11,
			MaximumPageSize:     22,
			MaximumLogLines:     33,
			MaximumMetricPoints: 44,
		},
	}, response)
}

func TestNewHandlerRejectsNilService(t *testing.T) {
	_, err := NewHandler(nil, &fakeObservabilityService{})
	require.Error(t, err)

	_, err = NewHandler(&fakeCapabilitiesService{}, nil)
	require.Error(t, err)
}

func TestGetPipelineVertexSummaryAndETag(t *testing.T) {
	summaryService := &fakeObservabilityService{
		summary: observability.Result[observability.VertexSummary]{
			ResourceVersion: "17",
			Value: observability.VertexSummary{
				Ref: observability.TargetRef{
					Kind:      observability.TargetKindVertex,
					Namespace: "team-a",
					Pipeline:  "orders",
					Name:      "map",
					UID:       "vertex-uid",
				},
				VertexType:         "MapUDF",
				Phase:              "Running",
				DesiredPhase:       "Running",
				Health:             observability.Health{State: observability.HealthStateHealthy},
				Generation:         4,
				ObservedGeneration: 4,
				CreatedAt:          time.Date(2026, time.September, 23, 10, 0, 0, 0, time.UTC),
				ObservedAt:         time.Date(2026, time.September, 23, 11, 0, 0, 0, time.UTC),
				Capabilities:       []string{"summary", "status"},
			},
		},
	}
	router := testRouter(t, &fakeCapabilitiesService{}, summaryService)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/namespaces/team-a/pipelines/orders/vertices/map/summary", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, `"17"`, recorder.Header().Get("ETag"))
	assert.Equal(t, "private, no-cache", recorder.Header().Get("Cache-Control"))
	assert.Less(t, recorder.Body.Len(), 2048)
	assert.NotContains(t, recorder.Body.String(), `"data"`)
	var response generated.VertexSummary
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, generated.TargetKindVertex, response.Ref.Kind)
	assert.Equal(t, "map", response.Ref.Name)
	assert.Equal(t, generated.VertexType("MapUDF"), response.VertexType)

	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/namespaces/team-a/pipelines/orders/vertices/map/summary", nil)
	request.Header.Set("If-None-Match", `"17"`)
	router.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusNotModified, recorder.Code)
	assert.Empty(t, recorder.Body.String())
}

func TestPipelineVertexSummaryProblems(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		serviceErr    error
		status        int
		problemCode   string
		problemDetail string
	}{
		{
			name:        "invalid Kubernetes name",
			path:        "/namespaces/TEAM_A/pipelines/orders/vertices/map/summary",
			status:      http.StatusUnprocessableEntity,
			problemCode: "validation_failed",
		},
		{
			name:          "not found",
			path:          "/namespaces/team-a/pipelines/orders/vertices/map/summary",
			serviceErr:    apierrors.NewNotFound(schema.GroupResource{Group: "numaflow.numaproj.io", Resource: "vertices"}, "orders-map"),
			status:        http.StatusNotFound,
			problemCode:   "target_not_found",
			problemDetail: "The requested vertex does not exist",
		},
		{
			name:        "provider failure",
			path:        "/namespaces/team-a/pipelines/orders/vertices/map/summary",
			serviceErr:  errors.New("provider unavailable"),
			status:      http.StatusInternalServerError,
			problemCode: "target_read_failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := testRouter(t, &fakeCapabilitiesService{}, &fakeObservabilityService{err: test.serviceErr})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))

			assert.Equal(t, test.status, recorder.Code)
			assert.Equal(t, "application/problem+json", recorder.Header().Get("Content-Type"))
			var problem generated.Problem
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
			assert.Equal(t, test.problemCode, problem.Code)
			if test.problemDetail != "" {
				assert.Equal(t, test.problemDetail, problem.Detail)
			}
			assert.NotContains(t, problem.Detail, "provider unavailable")
		})
	}
}

func TestGetPipelineVertexStatusAndETag(t *testing.T) {
	statusService := &fakeObservabilityService{
		status: observability.Result[observability.VertexStatus]{
			ResourceVersion: "18",
			Value: observability.VertexStatus{
				Ref: observability.TargetRef{
					Kind:      observability.TargetKindVertex,
					Namespace: "team-a",
					Pipeline:  "orders",
					Name:      "map",
					UID:       "vertex-uid",
				},
				Phase:              "Running",
				DesiredPhase:       "Running",
				Reason:             "Running",
				Message:            "Vertex is running",
				Replicas:           observability.ReplicaStatus{Current: 3, Desired: 4, Ready: 2, Updated: 3, UpdatedReady: 2},
				Conditions:         []observability.Condition{{Type: "PodsHealthy", Status: "True", Reason: "Ready", Message: "All pods are ready", ObservedGeneration: 4, LastTransitionTime: time.Date(2026, time.September, 23, 11, 0, 0, 0, time.UTC)}},
				Generation:         4,
				ObservedGeneration: 4,
				ObservedAt:         time.Date(2026, time.September, 23, 11, 0, 0, 0, time.UTC),
			},
		},
	}
	router := testRouter(t, &fakeCapabilitiesService{}, statusService)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/namespaces/team-a/pipelines/orders/vertices/map/status", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, `"18"`, recorder.Header().Get("ETag"))
	assert.Equal(t, "private, no-cache", recorder.Header().Get("Cache-Control"))
	assert.NotContains(t, recorder.Body.String(), `"data"`)
	var response generated.VertexStatus
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, generated.TargetKindVertex, response.Ref.Kind)
	assert.Equal(t, int64(4), response.Replicas.Desired)
	require.Len(t, response.Conditions, 1)
	assert.Equal(t, generated.ConditionStatus("True"), response.Conditions[0].Status)

	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/namespaces/team-a/pipelines/orders/vertices/map/status", nil)
	request.Header.Set("If-None-Match", `"18"`)
	router.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusNotModified, recorder.Code)
	assert.Empty(t, recorder.Body.String())
}

func TestPipelineVertexStatusProblems(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		serviceErr  error
		status      int
		problemCode string
	}{
		{
			name:        "invalid Kubernetes name",
			path:        "/namespaces/TEAM_A/pipelines/orders/vertices/map/status",
			status:      http.StatusUnprocessableEntity,
			problemCode: "validation_failed",
		},
		{
			name:        "not found",
			path:        "/namespaces/team-a/pipelines/orders/vertices/map/status",
			serviceErr:  apierrors.NewNotFound(schema.GroupResource{Group: "numaflow.numaproj.io", Resource: "vertices"}, "orders-map"),
			status:      http.StatusNotFound,
			problemCode: "target_not_found",
		},
		{
			name:        "provider failure",
			path:        "/namespaces/team-a/pipelines/orders/vertices/map/status",
			serviceErr:  errors.New("provider unavailable"),
			status:      http.StatusInternalServerError,
			problemCode: "target_read_failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := testRouter(t, &fakeCapabilitiesService{}, &fakeObservabilityService{err: test.serviceErr})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))

			assert.Equal(t, test.status, recorder.Code)
			assert.Equal(t, "application/problem+json", recorder.Header().Get("Content-Type"))
			var problem generated.Problem
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
			assert.Equal(t, test.problemCode, problem.Code)
			assert.NotContains(t, problem.Detail, "provider unavailable")
		})
	}
}

func TestGetMonoVertexSummaryAndETag(t *testing.T) {
	observabilityService := &fakeObservabilityService{
		monoSummary: observability.Result[observability.VertexSummary]{
			ResourceVersion: "19",
			Value: observability.VertexSummary{
				Ref: observability.TargetRef{
					Kind:      observability.TargetKindMonoVertex,
					Namespace: "team-a",
					Name:      "orders-ingest",
					UID:       "mono-vertex-uid",
				},
				VertexType:         "MonoVertex",
				Phase:              "Running",
				DesiredPhase:       "Running",
				Health:             observability.Health{State: observability.HealthStateHealthy},
				Generation:         4,
				ObservedGeneration: 4,
				CreatedAt:          time.Date(2026, time.September, 30, 10, 0, 0, 0, time.UTC),
				ObservedAt:         time.Date(2026, time.September, 30, 11, 0, 0, 0, time.UTC),
				Capabilities:       []string{"summary", "status"},
			},
		},
	}
	router := testRouter(t, &fakeCapabilitiesService{}, observabilityService)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/namespaces/team-a/mono-vertices/orders-ingest/summary", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, `"19"`, recorder.Header().Get("ETag"))
	var response generated.VertexSummary
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, generated.TargetKindMonoVertex, response.Ref.Kind)
	assert.Nil(t, response.Ref.Pipeline)
	assert.Equal(t, generated.VertexTypeMonoVertex, response.VertexType)

	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/namespaces/team-a/mono-vertices/orders-ingest/summary", nil)
	request.Header.Set("If-None-Match", `"19"`)
	router.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusNotModified, recorder.Code)
	assert.Empty(t, recorder.Body.String())
}

func TestGetMonoVertexStatusAndETag(t *testing.T) {
	observabilityService := &fakeObservabilityService{
		monoStatus: observability.Result[observability.VertexStatus]{
			ResourceVersion: "20",
			Value: observability.VertexStatus{
				Ref: observability.TargetRef{
					Kind:      observability.TargetKindMonoVertex,
					Namespace: "team-a",
					Name:      "orders-ingest",
					UID:       "mono-vertex-uid",
				},
				Phase:              "Running",
				DesiredPhase:       "Running",
				Replicas:           observability.ReplicaStatus{Current: 2, Desired: 2, Ready: 2, Updated: 2, UpdatedReady: 2},
				Conditions:         []observability.Condition{{Type: "DaemonHealthy", Status: "True", Reason: "Ready", Message: "Daemon is ready", ObservedGeneration: 4, LastTransitionTime: time.Date(2026, time.September, 30, 11, 0, 0, 0, time.UTC)}},
				Generation:         4,
				ObservedGeneration: 4,
				ObservedAt:         time.Date(2026, time.September, 30, 11, 0, 0, 0, time.UTC),
			},
		},
	}
	router := testRouter(t, &fakeCapabilitiesService{}, observabilityService)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/namespaces/team-a/mono-vertices/orders-ingest/status", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, `"20"`, recorder.Header().Get("ETag"))
	var response generated.VertexStatus
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Nil(t, response.Ref.Pipeline)
	assert.Equal(t, int64(2), response.Replicas.Ready)
	require.Len(t, response.Conditions, 1)
	assert.Equal(t, "DaemonHealthy", response.Conditions[0].Type)

	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/namespaces/team-a/mono-vertices/orders-ingest/status", nil)
	request.Header.Set("If-None-Match", `"20"`)
	router.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusNotModified, recorder.Code)
	assert.Empty(t, recorder.Body.String())
}

func TestMonoVertexProblems(t *testing.T) {
	endpoints := []string{"summary", "status"}
	tests := []struct {
		name        string
		path        string
		serviceErr  error
		status      int
		problemCode string
	}{
		{
			name:        "invalid Kubernetes name",
			path:        "/namespaces/TEAM_A/mono-vertices/orders-ingest",
			status:      http.StatusUnprocessableEntity,
			problemCode: "validation_failed",
		},
		{
			name:        "not found",
			path:        "/namespaces/team-a/mono-vertices/orders-ingest",
			serviceErr:  apierrors.NewNotFound(schema.GroupResource{Group: "numaflow.numaproj.io", Resource: "monovertices"}, "orders-ingest"),
			status:      http.StatusNotFound,
			problemCode: "target_not_found",
		},
		{
			name:        "provider failure",
			path:        "/namespaces/team-a/mono-vertices/orders-ingest",
			serviceErr:  errors.New("provider unavailable"),
			status:      http.StatusInternalServerError,
			problemCode: "target_read_failed",
		},
	}
	for _, endpoint := range endpoints {
		for _, test := range tests {
			t.Run(endpoint+"/"+test.name, func(t *testing.T) {
				router := testRouter(t, &fakeCapabilitiesService{}, &fakeObservabilityService{err: test.serviceErr})
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path+"/"+endpoint, nil))

				assert.Equal(t, test.status, recorder.Code)
				assert.Equal(t, "application/problem+json", recorder.Header().Get("Content-Type"))
				var problem generated.Problem
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
				assert.Equal(t, test.problemCode, problem.Code)
				assert.NotContains(t, problem.Detail, "provider unavailable")
			})
		}
	}
}

func testRouter(t *testing.T, capabilitiesService CapabilitiesService, observabilityService ObservabilityService) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	handler, err := NewHandler(capabilitiesService, observabilityService)
	require.NoError(t, err)
	router := gin.New()
	generated.RegisterHandlersWithOptions(router, handler, generated.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, _ int) {
			WriteProblem(c, http.StatusUnprocessableEntity, "validation_failed", "Request validation failed", err.Error(), nil)
		},
	})
	return router
}

type fakeCapabilitiesService struct {
	capabilities capabilities.Capabilities
}

func (f *fakeCapabilitiesService) GetCapabilities() capabilities.Capabilities {
	return f.capabilities
}

type fakeObservabilityService struct {
	summary     observability.Result[observability.VertexSummary]
	status      observability.Result[observability.VertexStatus]
	monoSummary observability.Result[observability.VertexSummary]
	monoStatus  observability.Result[observability.VertexStatus]
	err         error
}

func (f *fakeObservabilityService) GetPipelineVertexSummary(context.Context, string, string, string) (observability.Result[observability.VertexSummary], error) {
	return f.summary, f.err
}

func (f *fakeObservabilityService) GetPipelineVertexStatus(context.Context, string, string, string) (observability.Result[observability.VertexStatus], error) {
	return f.status, f.err
}

func (f *fakeObservabilityService) GetMonoVertexSummary(context.Context, string, string) (observability.Result[observability.VertexSummary], error) {
	return f.monoSummary, f.err
}

func (f *fakeObservabilityService) GetMonoVertexStatus(context.Context, string, string) (observability.Result[observability.VertexStatus], error) {
	return f.monoStatus, f.err
}
