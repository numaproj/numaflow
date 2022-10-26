package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func (ms *mockIsbSvcClient) GetBufferInfo(ctx context.Context, buffer v1alpha1.Buffer) (*isbsvc.BufferInfo, error) {
	return &isbsvc.BufferInfo{
		Name:            buffer.Name,
		PendingCount:    10,
		AckPendingCount: 15,
		TotalMessages:   20,
	}, nil
}

func (ms *mockIsbSvcClient) CreateBuffers(ctx context.Context, buffers []v1alpha1.Buffer, opts ...isbsvc.BufferCreateOption) error {
	return nil
}

func (ms *mockIsbSvcClient) DeleteBuffers(ctx context.Context, buffers []v1alpha1.Buffer) error {
	return nil
}

func (ms *mockIsbSvcClient) ValidateBuffers(ctx context.Context, buffers []v1alpha1.Buffer) error {
	return nil
}

func (ms *mockIsbSvcClient) CreateWatermarkFetcher(ctx context.Context, bufferName string) (fetch.Fetcher, error) {
	return nil, nil
}

func TestGetVertexMetrics(t *testing.T) {
	pipelineName := "simple-pipeline"
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: pipelineName},
	}
	client, _ := isbsvc.NewISBJetStreamSvc(pipelineName)
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(client, pipeline)
	assert.NoError(t, err)

	metricsResponse := `# HELP vertex_processing_rate Message processing rate in the last period of seconds, tps. It represents the rate of a vertex instead of a pod.
# TYPE vertex_processing_rate gauge
vertex_processing_rate{period="15m",pipeline="simple-pipeline",vertex="cat"} 4.894736842105263
vertex_processing_rate{period="1m",pipeline="simple-pipeline",vertex="cat"} 5.084745762711864
vertex_processing_rate{period="5m",pipeline="simple-pipeline",vertex="cat"} 4.894736842105263
vertex_processing_rate{period="default",pipeline="simple-pipeline",vertex="cat"} 4.894736842105263

# HELP vertex_pending_messages Average pending messages in the last period of seconds. It is the pending messages of a vertex, not a pod.
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
	assert.Equal(t, resp.Vertex.GetProcessingRates(), processingRates)

	pendings := make(map[string]int64)
	pendings["15m"] = 4
	pendings["1m"] = 5
	pendings["5m"] = 6
	pendings["default"] = 7
	assert.Equal(t, resp.Vertex.GetPendings(), pendings)
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
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(ms, pipeline)
	assert.NoError(t, err)

	bufferName := "numaflow-system-simple-pipeline-in-cat"

	req := &daemon.GetBufferRequest{Pipeline: &pipelineName, Buffer: &bufferName}

	resp, err := pipelineMetricsQueryService.GetBuffer(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, *resp.Buffer.BufferUsage, 0.0004)

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
				{Name: "in"},
				{Name: "cat"},
				{Name: "out"},
			},
			Edges: edges,
		},
	}

	ms := &mockIsbSvcClient{}
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(ms, pipeline)
	assert.NoError(t, err)

	req := &daemon.ListBuffersRequest{Pipeline: &pipelineName}

	resp, err := pipelineMetricsQueryService.ListBuffers(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, len(resp.Buffers), 2)
}
