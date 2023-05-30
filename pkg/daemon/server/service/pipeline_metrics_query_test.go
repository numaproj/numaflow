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

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
)

type mockGetType func(url string) (*http.Response, error)

type mockHttpClient struct {
	MockGet mockGetType
}

func (m *mockHttpClient) Get(url string) (*http.Response, error) {
	return m.MockGet(url)
}

type mockIsbSvcClient struct {
}

func (ms *mockIsbSvcClient) GetBufferInfo(ctx context.Context, buffer string) (*isbsvc.BufferInfo, error) {
	return &isbsvc.BufferInfo{
		Name:            buffer,
		PendingCount:    10,
		AckPendingCount: 15,
		TotalMessages:   20,
	}, nil
}

func (ms *mockIsbSvcClient) CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, opts ...isbsvc.CreateOption) error {
	return nil
}

func (ms *mockIsbSvcClient) DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string) error {
	return nil
}

func (ms *mockIsbSvcClient) ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string) error {
	return nil
}

func (ms *mockIsbSvcClient) CreateWatermarkFetcher(ctx context.Context, bucketName string, partitions int, isReduce bool) ([]fetch.Fetcher, error) {
	return nil, nil
}

// mock rater
type mockRater_TestGetVertexMetrics struct {
}

func (mr *mockRater_TestGetVertexMetrics) Start(ctx context.Context) error {
	return nil
}

func (mr *mockRater_TestGetVertexMetrics) GetRates(vertexName string) map[string]float64 {
	res := make(map[string]float64)
	res["default"] = 4.894736842105263
	res["1m"] = 5.084745762711864
	res["5m"] = 4.894736842105263
	res["15m"] = 4.894736842105263
	return res
}

func (mr *mockRater_TestGetVertexMetrics) GetPodRates(vertexName string, podIndex int) map[string]float64 {
	return nil
}

func TestGetVertexMetrics(t *testing.T) {
	pipelineName := "simple-pipeline"
	vertexName := "cat"
	vertexPartition := int32(1)
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: pipelineName},
		Spec:       v1alpha1.PipelineSpec{Vertices: []v1alpha1.AbstractVertex{{Name: vertexName, Partitions: &vertexPartition}}},
	}
	client, _ := isbsvc.NewISBJetStreamSvc(pipelineName)
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(client, pipeline, nil, &mockRater_TestGetVertexMetrics{})
	assert.NoError(t, err)

	metricsResponse := `# HELP vertex_pending_messages Average pending messages in the last period of seconds. It is the pending messages of a vertex, not a pod.
# TYPE vertex_pending_messages gauge
vertex_pending_messages{period="15m",pipeline="simple-pipeline",vertex="cat"} 4.011
vertex_pending_messages{period="1m",pipeline="simple-pipeline",vertex="cat"} 5.333
vertex_pending_messages{period="5m",pipeline="simple-pipeline",vertex="cat"} 6.002
vertex_pending_messages{period="default",pipeline="simple-pipeline",vertex="cat"} 7.00002
`
	ioReader := io.NopCloser(bytes.NewReader([]byte(metricsResponse)))

	pipelineMetricsQueryService.httpClient = &mockHttpClient{
		MockGet: func(url string) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       ioReader,
			}, nil
		},
	}

	vertex := "cat"

	req := &daemon.GetVertexMetricsRequest{Vertex: &vertex}

	resp, err := pipelineMetricsQueryService.GetVertexMetrics(context.Background(), req)
	assert.NoError(t, err)

	processingRates := make(map[string]float64)

	processingRates["15m"] = 4.894736842105263
	processingRates["1m"] = 5.084745762711864
	processingRates["5m"] = 4.894736842105263
	processingRates["default"] = 4.894736842105263
	assert.Equal(t, resp.VertexMetrics[0].GetProcessingRates(), processingRates)

	pendings := make(map[string]int64)
	pendings["15m"] = 4
	pendings["1m"] = 5
	pendings["5m"] = 6
	pendings["default"] = 7
	assert.Equal(t, resp.VertexMetrics[0].GetPendings(), pendings)
}

func TestGetBuffer(t *testing.T) {
	pipelineName := "simple-pipeline"
	namespace := "numaflow-system"
	edges := []v1alpha1.Edge{
		{
			From: "in",
			To:   "cat",
		},
	}
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipelineName,
			Namespace: namespace,
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{Name: "in"},
				{Name: "cat"},
			},
			Edges: edges,
		},
	}

	ms := &mockIsbSvcClient{}
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(ms, pipeline, nil, nil)
	assert.NoError(t, err)

	bufferName := "numaflow-system-simple-pipeline-cat-0"

	req := &daemon.GetBufferRequest{Pipeline: &pipelineName, Buffer: &bufferName}

	resp, err := pipelineMetricsQueryService.GetBuffer(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, *resp.Buffer.BufferUsage, 0.0006666666666666666)
}

func TestListBuffers(t *testing.T) {
	pipelineName := "simple-pipeline"
	namespace := "numaflow-system"
	edges := []v1alpha1.Edge{
		{
			From: "in",
			To:   "cat",
		},
		{
			From: "cat",
			To:   "out",
		},
	}
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipelineName,
			Namespace: namespace,
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{Name: "in", Source: &v1alpha1.Source{}},
				{Name: "cat", UDF: &v1alpha1.UDF{}},
				{Name: "out", Sink: &v1alpha1.Sink{}},
			},
			Edges: edges,
		},
	}

	ms := &mockIsbSvcClient{}
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(ms, pipeline, nil, nil)
	assert.NoError(t, err)

	req := &daemon.ListBuffersRequest{Pipeline: &pipelineName}

	resp, err := pipelineMetricsQueryService.ListBuffers(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, len(resp.Buffers), 2)
}

// mock rater
type mockRater_TestGetPipelineStatus struct {
	isActivelyProcessing bool
}

func (mr *mockRater_TestGetPipelineStatus) Start(ctx context.Context) error {
	return nil
}

func (mr *mockRater_TestGetPipelineStatus) GetRates(vertexName string) map[string]float64 {
	res := make(map[string]float64)
	if mr.isActivelyProcessing {
		res["default"] = 4.894736842105263
		res["1m"] = 5.084745762711864
		res["5m"] = 4.894736842105263
		res["15m"] = 4.894736842105263
	} else {
		res["default"] = 0
		res["1m"] = 0
		res["5m"] = 0
		res["15m"] = 0
	}
	return res
}

func (mr *mockRater_TestGetPipelineStatus) GetPodRates(vertexName string, podIndex int) map[string]float64 {
	return nil
}

func TestGetPipelineStatus(t *testing.T) {
	pipelineName := "simple-pipeline"
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name: pipelineName,
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{Name: "cat"},
			},
		},
	}
	client, _ := isbsvc.NewISBJetStreamSvc(pipelineName)
	metricsResponse := `# HELP vertex_pending_messages Average pending messages in the last period of seconds. It is the pending messages of a vertex, not a pod.
# TYPE vertex_pending_messages gauge
vertex_pending_messages{period="15m",pipeline="simple-pipeline",vertex="cat"} 4.011
vertex_pending_messages{period="1m",pipeline="simple-pipeline",vertex="cat"} 5.333
vertex_pending_messages{period="5m",pipeline="simple-pipeline",vertex="cat"} 6.002
vertex_pending_messages{period="default",pipeline="simple-pipeline",vertex="cat"} 7.00002
`
	req := &daemon.GetPipelineStatusRequest{Pipeline: &pipelineName}

	// test when rater is actively processing
	activeRater := &mockRater_TestGetPipelineStatus{isActivelyProcessing: true}
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(client, pipeline, nil, activeRater)
	assert.NoError(t, err)
	ioReader := io.NopCloser(bytes.NewReader([]byte(metricsResponse)))
	pipelineMetricsQueryService.httpClient = &mockHttpClient{
		MockGet: func(url string) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       ioReader,
			}, nil
		},
	}
	resp, err := pipelineMetricsQueryService.GetPipelineStatus(context.Background(), req)
	assert.NoError(t, err)
	OKPipelineResponse := daemon.PipelineStatus{Status: pointer.String("OK"), Message: pointer.String("Pipeline has no issue.")}
	assert.Equal(t, &OKPipelineResponse, resp.Status)

	// test when rater is not actively processing
	idleRater := &mockRater_TestGetPipelineStatus{isActivelyProcessing: false}
	pipelineMetricsQueryService, err = NewPipelineMetadataQuery(client, pipeline, nil, idleRater)
	assert.NoError(t, err)
	ioReader = io.NopCloser(bytes.NewReader([]byte(metricsResponse)))
	pipelineMetricsQueryService.httpClient = &mockHttpClient{
		MockGet: func(url string) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       ioReader,
			}, nil
		},
	}
	resp, err = pipelineMetricsQueryService.GetPipelineStatus(context.Background(), req)
	assert.NoError(t, err)
	ErrorPipelineResponse := daemon.PipelineStatus{Status: pointer.String("Error"), Message: pointer.String("Pipeline has an error. Vertex cat is not processing pending messages.")}
	assert.Equal(t, &ErrorPipelineResponse, resp.Status)
}
