package service

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
)

type MockGetType func(url string) (*http.Response, error)

type MockClient struct {
	MockGet MockGetType
}

func (m *MockClient) Get(url string) (*http.Response, error) {
	return m.MockGet(url)

}

func TestGetVertexMetrics(t *testing.T) {
	pipelineName := "simple-pipeline"
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: pipelineName},
	}
	client, _ := isbsvc.NewISBJetStreamSvc(pipelineName)
	isbsQueryService := NewPipelineMetricsQueryService(client, pipeline)

	metricsResponse := `# HELP vertex_processing_rate Message processing rate in the last period of seconds, tps. It represents the rate of a vertex instead of a pod.
# TYPE vertex_processing_rate gauge
vertex_processing_rate{period="15m",pipeline="simple-pipeline",vertex="cat"} 4.894736842105263
vertex_processing_rate{period="1m",pipeline="simple-pipeline",vertex="cat"} 5.084745762711864
vertex_processing_rate{period="5m",pipeline="simple-pipeline",vertex="cat"} 4.894736842105263
vertex_processing_rate{period="default",pipeline="simple-pipeline",vertex="cat"} 4.894736842105263
`
	ioReader := ioutil.NopCloser(bytes.NewReader([]byte(metricsResponse)))

	isbsQueryService.httpClient = &MockClient{
		MockGet: func(url string) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       ioReader,
			}, nil
		},
	}

	vertex := "cat"
	namespace := "numaflow-system"

	req := &daemon.GetVertexMetricsRequest{Vertex: &vertex, Namespace: &namespace}

	resp, err := isbsQueryService.GetVertexMetrics(context.Background(), req)
	assert.NoError(t, err)

	processingRates := make(map[string]float32)

	processingRates["15m"] = 4.894737
	processingRates["1m"] = 5.084746
	processingRates["5m"] = 4.894737
	processingRates["default"] = 4.894737
	assert.Equal(t, resp.Vertex.GetProcessingRates(), processingRates)
}
