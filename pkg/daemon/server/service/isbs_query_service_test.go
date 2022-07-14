package service

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

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

func TestGetVertex(t *testing.T) {
	pipelineName := "simple-pipeline"
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: pipelineName},
	}
	client, _ := isbsvc.NewISBJetStreamSvc(pipelineName)
	isbsQueryService := NewISBSvcQueryService(client, pipeline)

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

	req := &daemon.GetVertexRequest{Vertex: &vertex, Namespace: &namespace}

	resp, err := isbsQueryService.GetVertex(context.Background(), req)
	assert.NoError(t, err)

	processingRates := make([]*daemon.ProcessingRate, 0)
	processingRate1 := &daemon.ProcessingRate{
		Lookback: pointer.String("15m"),
		Rate:     pointer.Float32(4.894737),
	}
	processingRate2 := &daemon.ProcessingRate{
		Lookback: pointer.String("1m"),
		Rate:     pointer.Float32(5.084746),
	}
	processingRate3 := &daemon.ProcessingRate{
		Lookback: pointer.String("5m"),
		Rate:     pointer.Float32(4.894737),
	}
	processingRate4 := &daemon.ProcessingRate{
		Lookback: pointer.String("default"),
		Rate:     pointer.Float32(4.894737),
	}

	processingRates = append(processingRates, processingRate1, processingRate2, processingRate3, processingRate4)
	assert.Equal(t, resp.Vertex.GetProcessingRate(), processingRates)
}
