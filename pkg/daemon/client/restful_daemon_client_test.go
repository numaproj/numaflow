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

package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRESTfulDaemonServiceClient(t *testing.T) {
	client, err := NewRESTfulDaemonServiceClient("localhost:8080")
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.IsType(t, &restfulDaemonClient{}, client)
}

func TestRestfulDaemonClient_IsDrained(t *testing.T) {
	t.Run("not drained", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"buffers":[{"pipeline":"simple-pipeline","bufferName":"numaflow-system-simple-pipeline-cat-0","pendingCount":"0","ackPendingCount":"5","totalMessages":"5","bufferLength":"30000","bufferUsageLimit":0.8,"bufferUsage":0.00016666666666666666,"isFull":false},{"pipeline":"simple-pipeline","bufferName":"numaflow-system-simple-pipeline-out-0","pendingCount":"0","ackPendingCount":"0","totalMessages":"0","bufferLength":"30000","bufferUsageLimit":0.8,"bufferUsage":0,"isFull":false}]}`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		isDrained, err := client.IsDrained(context.Background(), "simple-pipeline")
		assert.NoError(t, err)
		assert.False(t, isDrained)
	})

	t.Run("drained", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"buffers":[{"pipeline":"simple-pipeline","bufferName":"numaflow-system-simple-pipeline-cat-0","pendingCount":"0","ackPendingCount":"0","totalMessages":"5","bufferLength":"30000","bufferUsageLimit":0.8,"bufferUsage":0,"isFull":false},{"pipeline":"simple-pipeline","bufferName":"numaflow-system-simple-pipeline-out-0","pendingCount":"0","ackPendingCount":"0","totalMessages":"0","bufferLength":"30000","bufferUsageLimit":0.8,"bufferUsage":0,"isFull":false}]}`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		isDrained, err := client.IsDrained(context.Background(), "simple-pipeline")
		assert.NoError(t, err)
		assert.True(t, isDrained)
	})
}

func TestRestfulDaemonClient_ListPipelineBuffers(t *testing.T) {
	t.Run("error case", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`invalid json`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		_, err := client.ListPipelineBuffers(context.Background(), "simple-pipeline")
		assert.Error(t, err)
	})

	t.Run("okay", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"buffers":[{"pipeline":"simple-pipeline","bufferName":"numaflow-system-simple-pipeline-cat-0","pendingCount":"0","ackPendingCount":"5","totalMessages":"5","bufferLength":"30000","bufferUsageLimit":0.8,"bufferUsage":0.00016666666666666666,"isFull":false},{"pipeline":"simple-pipeline","bufferName":"numaflow-system-simple-pipeline-out-0","pendingCount":"0","ackPendingCount":"0","totalMessages":"0","bufferLength":"30000","bufferUsageLimit":0.8,"bufferUsage":0,"isFull":false}]}`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		buffers, err := client.ListPipelineBuffers(context.Background(), "simple-pipeline")
		assert.NoError(t, err)
		assert.Len(t, buffers, 2)
		assert.Equal(t, "numaflow-system-simple-pipeline-cat-0", buffers[0].BufferName)
		assert.Equal(t, "numaflow-system-simple-pipeline-out-0", buffers[1].BufferName)
	})
}

func TestRestfulDaemonClient_GetPipelineBuffer(t *testing.T) {
	t.Run("error case", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`invalid json`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		_, err := client.GetPipelineBuffer(context.Background(), "simple-pipeline", "numaflow-system-simple-pipeline-cat-0")
		assert.Error(t, err)
	})

	t.Run("okay", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"buffer":{"pipeline":"simple-pipeline","bufferName":"numaflow-system-simple-pipeline-cat-0","pendingCount":"0","ackPendingCount":"0","totalMessages":"0","bufferLength":"30000","bufferUsageLimit":0.8,"bufferUsage":0,"isFull":false}}`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		buffer, err := client.GetPipelineBuffer(context.Background(), "simple-pipeline", "numaflow-system-simple-pipeline-cat-0")
		assert.NoError(t, err)
		assert.NotNil(t, buffer)
		assert.Equal(t, "numaflow-system-simple-pipeline-cat-0", buffer.BufferName)
		assert.Equal(t, int64(0), buffer.PendingCount.GetValue())
	})
}

func TestRestfulDaemonClient_GetVertexMetrics(t *testing.T) {
	t.Run("error case", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`invalid json`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		_, err := client.GetVertexMetrics(context.Background(), "simple-pipeline", "out")
		assert.Error(t, err)
	})

	t.Run("okay", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"vertexMetrics":[{"pipeline":"simple-pipeline","vertex":"out","processingRates":{"15m":5.007407407407407,"1m":5.083333333333333,"5m":5.007407407407407,"default":5.033333333333333},"pendings":{"15m":"0","1m":"0","5m":"0","default":"0"}}]}`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		metrics, err := client.GetVertexMetrics(context.Background(), "simple-pipeline", "out")
		assert.NoError(t, err)
		assert.Len(t, metrics, 1)
		assert.Equal(t, "simple-pipeline", metrics[0].Pipeline)
		assert.Equal(t, "out", metrics[0].Vertex)
	})
}

func TestRestfulDaemonClient_GetPipelineWatermarks(t *testing.T) {
	t.Run("error case", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`invalid json`))
		}))
		defer server.Close()
		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		_, err := client.GetPipelineWatermarks(context.Background(), "test-pipeline")
		assert.Error(t, err)
	})

	t.Run("okay", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"pipelineWatermarks":[{"pipeline":"simple-pipeline","edge":"in-cat","watermarks":["1720992797298"],"isWatermarkEnabled":true,"from":"in","to":"cat"},{"pipeline":"simple-pipeline","edge":"cat-out","watermarks":["1720992796298"],"isWatermarkEnabled":true,"from":"cat","to":"out"}]}`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		watermarks, err := client.GetPipelineWatermarks(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.Len(t, watermarks, 2)
		assert.Equal(t, "in-cat", watermarks[0].Edge)
		assert.Equal(t, int64(1720992797298), watermarks[0].Watermarks[0].GetValue())
		assert.Equal(t, "cat-out", watermarks[1].Edge)
		assert.Equal(t, int64(1720992796298), watermarks[1].Watermarks[0].GetValue())
	})
}

func TestRestfulDaemonClient_GetPipelineStatus(t *testing.T) {
	t.Run("error case", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`invalid json`))
		}))
		defer server.Close()
		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		_, err := client.GetPipelineStatus(context.Background(), "test-pipeline")
		assert.Error(t, err)
	})

	t.Run("okay", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":{"status":"healthy","message":"Pipeline data flow is healthy","code":"D1"}}`))
		}))
		defer server.Close()

		client, _ := NewRESTfulDaemonServiceClient(server.URL)
		status, err := client.GetPipelineStatus(context.Background(), "test-pipeline")
		assert.NoError(t, err)
		assert.NotNil(t, status)
		assert.Equal(t, "healthy", status.Status)
	})
}

func TestRestfulDaemonClient_Close(t *testing.T) {
	t.Run("close without error", func(t *testing.T) {
		client := &restfulDaemonClient{}
		err := client.Close()
		assert.NoError(t, err)
	})
}

func TestUnmarshalResponse(t *testing.T) {
	t.Run("successful unmarshal", func(t *testing.T) {
		response := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"name":"test","value":123}`)),
		}
		type testStruct struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}
		result, err := unmarshalResponse[testStruct](response)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, 123, result.Value)
	})

	t.Run("error status code", func(t *testing.T) {
		response := &http.Response{
			StatusCode: http.StatusBadRequest,
			Status:     "400 Bad Request",
		}
		result, err := unmarshalResponse[struct{}](response)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unexpected response 400")
	})

	t.Run("error reading body", func(t *testing.T) {
		response := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(errReader(0)),
		}
		result, err := unmarshalResponse[struct{}](response)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to read data from response body")
	})

	t.Run("error unmarshalling", func(t *testing.T) {
		response := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"invalid json"`)),
		}
		result, err := unmarshalResponse[struct{}](response)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to unmarshal response body")
	})
}

type errReader int

func (errReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("test error")
}
