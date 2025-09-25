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

package watermark

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// MockHTTPClient is a mock implementation of HTTPClient interface
type MockHTTPClient struct {
	mock.Mock
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

func TestWatermarkResponse_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		expected map[string]int64
		wantErr  bool
	}{
		{
			name:     "valid single partition",
			jsonData: `{"0": 1752314641078}`,
			expected: map[string]int64{"0": 1752314641078},
			wantErr:  false,
		},
		{
			name:     "valid multiple partitions",
			jsonData: `{"0": 1752314641078, "1": 1752314641079, "2": 1752314641080}`,
			expected: map[string]int64{"0": 1752314641078, "1": 1752314641079, "2": 1752314641080},
			wantErr:  false,
		},
		{
			name:     "watermark with -1 value",
			jsonData: `{"0": -1, "1": 1752314641079}`,
			expected: map[string]int64{"0": -1, "1": 1752314641079},
			wantErr:  false,
		},
		{
			name:     "empty response",
			jsonData: `{}`,
			expected: map[string]int64{},
			wantErr:  false,
		},
		{
			name:     "invalid json",
			jsonData: `{"0": invalid}`,
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp Response
			err := resp.UnmarshalJSON([]byte(tt.jsonData))

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, resp.Partitions)
			}
		})
	}
}

func TestNewHTTPWatermarkFetcher(t *testing.T) {
	ctx := context.Background()
	pipeline := createTestPipeline()
	edge := v1alpha1.Edge{From: "source", To: "map"}

	fetcher := NewHTTPWatermarkFetcher(ctx, pipeline, edge)
	defer fetcher.Stop() // Clean up background goroutine

	assert.NotNil(t, fetcher)
	assert.Equal(t, pipeline, fetcher.pipeline)
	assert.Equal(t, edge, fetcher.edge)
	assert.NotNil(t, fetcher.httpClient)
	assert.NotNil(t, fetcher.log)
	assert.NotNil(t, fetcher.watermarkCache)
	assert.NotNil(t, fetcher.fetchTicker)
	assert.NotNil(t, fetcher.ctx)
	assert.NotNil(t, fetcher.cancel)
}

func TestHTTPWatermarkFetcher_fetchWatermarkFromURL(t *testing.T) {
	ctx := context.Background()
	pipeline := createTestPipeline()
	edge := v1alpha1.Edge{From: "source", To: "map"}

	fetcher := NewHTTPWatermarkFetcher(ctx, pipeline, edge)
	defer fetcher.Stop() // Clean up background goroutine

	tests := []struct {
		name           string
		responseBody   string
		statusCode     int
		expectedResult map[string]int64
		expectError    bool
	}{
		{
			name:           "successful response",
			responseBody:   `{"0": 1752314641078, "1": 1752314641079}`,
			statusCode:     200,
			expectedResult: map[string]int64{"0": 1752314641078, "1": 1752314641079},
			expectError:    false,
		},
		{
			name:           "response with -1 watermark",
			responseBody:   `{"0": -1}`,
			statusCode:     200,
			expectedResult: map[string]int64{"0": -1},
			expectError:    false,
		},
		{
			name:           "HTTP error status",
			responseBody:   "",
			statusCode:     500,
			expectedResult: nil,
			expectError:    true,
		},
		{
			name:           "invalid JSON response",
			responseBody:   `{"0": invalid}`,
			statusCode:     200,
			expectedResult: nil,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP client
			mockClient := &MockHTTPClient{}
			fetcher.httpClient = mockClient

			// Setup mock response
			resp := &http.Response{
				StatusCode: tt.statusCode,
				Body:       io.NopCloser(bytes.NewReader([]byte(tt.responseBody))),
			}

			mockClient.On("Do", mock.AnythingOfType("*http.Request")).Return(resp, nil)

			result, err := fetcher.fetchFromURL(ctx, "https://test-url/watermark")

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestHTTPWatermarkFetcher_CacheOperations(t *testing.T) {
	ctx := context.Background()
	pipeline := createTestPipeline()
	edge := v1alpha1.Edge{From: "source", To: "map"}

	fetcher := NewHTTPWatermarkFetcher(ctx, pipeline, edge)
	defer fetcher.Stop() // Clean up background goroutine

	// Test initial cache state (should be -1 for initialized partitions)
	assert.Equal(t, int64(-1), fetcher.getCached(0))

	// Test updating cache
	fetcher.updateCache(0, 1752314641078)
	assert.Equal(t, int64(1752314641078), fetcher.getCached(0))

	// Test multiple partitions
	fetcher.updateCache(1, 1752314641079)
	fetcher.updateCache(2, 1752314641080)

	assert.Equal(t, int64(1752314641078), fetcher.getCached(0))
	assert.Equal(t, int64(1752314641079), fetcher.getCached(1))
	assert.Equal(t, int64(1752314641080), fetcher.getCached(2))

	// Test getting cached watermarks for all partitions
	watermarks := fetcher.getCachedWatermarks(3)
	assert.Len(t, watermarks, 3)
	assert.Equal(t, int64(1752314641078), watermarks[0].GetValue())
	assert.Equal(t, int64(1752314641079), watermarks[1].GetValue())
	assert.Equal(t, int64(1752314641080), watermarks[2].GetValue())
}

func TestHTTPWatermarkFetcher_GetEdgeWatermarksFromCache(t *testing.T) {
	ctx := context.Background()
	pipeline := createTestPipeline()
	edge := v1alpha1.Edge{From: "source", To: "map"}

	fetcher := NewHTTPWatermarkFetcher(ctx, pipeline, edge)
	defer fetcher.Stop() // Clean up background goroutine

	// Get the actual partition count for the map vertex
	toVertex := pipeline.GetVertex(edge.To)
	partitionCount := toVertex.GetPartitionCount()

	// Update cache with some values
	fetcher.updateCache(0, 1752314641078)
	if partitionCount > 1 {
		fetcher.updateCache(1, 1752314641079)
	}

	// Test GetWatermarks returns cached values
	watermarks, err := fetcher.GetWatermarks()
	assert.NoError(t, err)
	assert.Len(t, watermarks, partitionCount)
	assert.Equal(t, int64(1752314641078), watermarks[0].GetValue())
	if partitionCount > 1 {
		assert.Equal(t, int64(1752314641079), watermarks[1].GetValue())
	}
}

func TestHTTPWatermarkFetcher_Stop(t *testing.T) {
	ctx := context.Background()
	pipeline := createTestPipeline()
	edge := v1alpha1.Edge{From: "source", To: "map"}

	fetcher := NewHTTPWatermarkFetcher(ctx, pipeline, edge)

	// Verify fetcher is running
	assert.NotNil(t, fetcher.ctx)
	assert.NotNil(t, fetcher.cancel)
	assert.NotNil(t, fetcher.fetchTicker)

	// Stop the fetcher
	fetcher.Stop()

	// Verify context is cancelled
	select {
	case <-fetcher.ctx.Done():
		// Expected - context should be cancelled
	default:
		t.Error("Context should be cancelled after Stop()")
	}
}

// Helper function to create a test pipeline
func createTestPipeline() *v1alpha1.Pipeline {
	return &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pipeline",
			Namespace: "default",
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{
					Name:   "source",
					Source: &v1alpha1.Source{},
				},
				{
					Name:       "map",
					UDF:        &v1alpha1.UDF{},
					Partitions: &[]int32{2}[0],
				},
				{
					Name: "reduce",
					UDF: &v1alpha1.UDF{
						GroupBy: &v1alpha1.GroupBy{},
					},
					Partitions: &[]int32{3}[0],
				},
				{
					Name: "sink",
					Sink: &v1alpha1.Sink{},
				},
			},
			Edges: []v1alpha1.Edge{
				{From: "source", To: "map"},
				{From: "map", To: "reduce"},
				{From: "reduce", To: "sink"},
			},
		},
	}
}
