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

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	nats2 "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
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

func (ms *mockIsbSvcClient) GetBufferInfo(_ context.Context, buffer string) (*isbsvc.BufferInfo, error) {
	return &isbsvc.BufferInfo{
		Name:            buffer,
		PendingCount:    10,
		AckPendingCount: 15,
		TotalMessages:   20,
	}, nil
}

func (ms *mockIsbSvcClient) CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string, opts ...isbsvc.CreateOption) error {
	return nil
}

func (ms *mockIsbSvcClient) DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string) error {
	return nil
}

func (ms *mockIsbSvcClient) ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string) error {
	return nil
}

// mock rater
type mockRater_TestGetVertexMetrics struct {
}

func (mr *mockRater_TestGetVertexMetrics) Start(ctx context.Context) error {
	return nil
}

func (mr *mockRater_TestGetVertexMetrics) GetRates(vertexName string, partitionName string) map[string]*wrapperspb.DoubleValue {
	res := make(map[string]*wrapperspb.DoubleValue)
	res["default"] = wrapperspb.Double(4.894736842105263)
	res["1m"] = wrapperspb.Double(5.084745762711864)
	res["5m"] = wrapperspb.Double(4.894736842105263)
	res["15m"] = wrapperspb.Double(4.894736842105263)
	return res
}

func (mr *mockRater_TestGetVertexMetrics) GetPending(pipelinename, vertexName, vertexType, partitionName string) map[string]*wrapperspb.Int64Value {
	res := make(map[string]*wrapperspb.Int64Value)
	res["default"] = wrapperspb.Int64(7)
	res["1m"] = wrapperspb.Int64(5)
	res["5m"] = wrapperspb.Int64(6)
	res["15m"] = wrapperspb.Int64(4)
	return res
}

func TestGetVertexMetrics(t *testing.T) {
	s := test.RunJetStreamServer(t)
	defer test.ShutdownJetStreamServer(t, s)

	// write some messages to the stream
	conn, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer conn.Close()

	jsc := nats2.NewTestClient(t, s.ClientURL())

	pipelineName := "simple-pipeline"
	vertexName := "cat"
	vertexPartition := int32(1)
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: pipelineName},
		Spec:       v1alpha1.PipelineSpec{Vertices: []v1alpha1.AbstractVertex{{Name: vertexName, Partitions: &vertexPartition}}},
	}
	client, _ := isbsvc.NewISBJetStreamSvc(jsc)
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(context.Background(), client, pipeline, &mockRater_TestGetVertexMetrics{}, nil)
	assert.NoError(t, err)

	metricsResponse := `# HELP vertex_pending_messages Average pending messages in the last period of seconds. It is the pending messages of a vertex, not a pod.
# TYPE vertex_pending_messages gauge
vertex_pending_messages{period="15m",partition_name="-simple-pipeline-cat-0",pipeline="simple-pipeline",vertex="cat"} 4.011
vertex_pending_messages{period="1m",partition_name="-simple-pipeline-cat-0",pipeline="simple-pipeline",vertex="cat"} 5.333
vertex_pending_messages{period="5m",partition_name="-simple-pipeline-cat-0",pipeline="simple-pipeline",vertex="cat"} 6.002
vertex_pending_messages{period="default",partition_name="-simple-pipeline-cat-0",pipeline="simple-pipeline",vertex="cat"} 7.00002
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

	req := &daemon.GetVertexMetricsRequest{Pipeline: pipelineName, Vertex: vertex}

	resp, err := pipelineMetricsQueryService.GetVertexMetrics(context.Background(), req)
	assert.NoError(t, err)

	processingRates := make(map[string]*wrapperspb.DoubleValue)

	processingRates["15m"] = wrapperspb.Double(4.894736842105263)
	processingRates["1m"] = wrapperspb.Double(5.084745762711864)
	processingRates["5m"] = wrapperspb.Double(4.894736842105263)
	processingRates["default"] = wrapperspb.Double(4.894736842105263)
	assert.Equal(t, resp.VertexMetrics[0].GetProcessingRates(), processingRates)

	pendings := make(map[string]*wrapperspb.Int64Value)
	pendings["15m"] = wrapperspb.Int64(4)
	pendings["1m"] = wrapperspb.Int64(5)
	pendings["5m"] = wrapperspb.Int64(6)
	pendings["default"] = wrapperspb.Int64(7)
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
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(context.Background(), ms, pipeline, nil, nil)
	assert.NoError(t, err)

	bufferName := "numaflow-system-simple-pipeline-cat-0"

	req := &daemon.GetBufferRequest{Pipeline: pipelineName, Buffer: bufferName}

	resp, err := pipelineMetricsQueryService.GetBuffer(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, resp.Buffer.BufferUsage.GetValue(), 0.0006666666666666666)
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
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(context.Background(), ms, pipeline, nil, nil)
	assert.NoError(t, err)

	req := &daemon.ListBuffersRequest{Pipeline: pipelineName}

	resp, err := pipelineMetricsQueryService.ListBuffers(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, len(resp.Buffers), 2)
}
