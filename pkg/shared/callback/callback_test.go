package callback

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
)

func TestNonSinkVertexCallback(t *testing.T) {
	expected := []map[string]interface{}{
		{
			"vertex":      "testVertex",
			"pipeline":    "testPipeline",
			"id":          "XXXX",
			"from_vertex": "from-vertex",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// get request body and convert it to the text
		bodyBytes, _ := io.ReadAll(req.Body)
		var actual []map[string]interface{}
		err := json.Unmarshal(bodyBytes, &actual)
		assert.Nil(t, err)

		// remove the cb_time field
		for _, item := range actual {
			delete(item, "cb_time")
		}

		// compare the expected and actual body
		assert.Equal(t, expected, actual)

		// send 204 response
		rw.WriteHeader(http.StatusNoContent)
	}))

	defer server.Close()

	ctx := context.Background()
	cp := NewPublisher(ctx, "testVertex", "testPipeline", WithCallbackURL(server.URL))

	messagePairs := []isb.ReadWriteMessagePair{
		{
			ReadMessage: &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{
						Headers: map[string]string{
							dfv1.KeyMetaCallbackURL: server.URL,
							dfv1.KeyMetaID:          "XXXX",
						},
						ID: isb.MessageID{
							VertexName: "from-vertex",
						},
					},
				},
			},
			WriteMessages: []*isb.WriteMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							Headers: map[string]string{
								dfv1.KeyMetaCallbackURL: server.URL,
								dfv1.KeyMetaID:          "XXXX",
							},
						},
					},
				},
			},
		},
	}

	err := cp.NonSinkVertexCallback(ctx, messagePairs)
	assert.Nil(t, err)
}

func TestNonSinkVertexCallback_NoCallbackURL(t *testing.T) {
	ctx := context.Background()
	cp := NewPublisher(ctx, "testVertex", "testPipeline")

	messagePairs := []isb.ReadWriteMessagePair{
		{
			ReadMessage: &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{
						Headers: map[string]string{
							dfv1.KeyMetaID: "XXXX",
						},
						ID: isb.MessageID{
							VertexName: "from-vertex",
						},
					},
				},
			},
			WriteMessages: []*isb.WriteMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							Headers: map[string]string{
								dfv1.KeyMetaID: "XXXX",
							},
						},
					},
				},
			},
		},
	}

	err := cp.NonSinkVertexCallback(ctx, messagePairs)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "callback URL not found in headers and default callback URL is not set")
}

func TestNonSinkVertexCallback_CallbackURLSet(t *testing.T) {
	expected := []map[string]interface{}{
		{
			"vertex":      "testVertex",
			"pipeline":    "testPipeline",
			"id":          "XXXX",
			"from_vertex": "from-vertex",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		bodyBytes, _ := io.ReadAll(req.Body)
		var actual []map[string]interface{}
		err := json.Unmarshal(bodyBytes, &actual)
		assert.Nil(t, err)

		for _, item := range actual {
			delete(item, "cb_time")
		}

		assert.Equal(t, expected, actual)
		rw.WriteHeader(http.StatusNoContent)
	}))

	defer server.Close()

	ctx := context.Background()
	cp := NewPublisher(ctx, "testVertex", "testPipeline", WithCallbackURL(server.URL))

	messagePairs := []isb.ReadWriteMessagePair{
		{
			ReadMessage: &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{
						Headers: map[string]string{
							dfv1.KeyMetaID: "XXXX",
						},
						ID: isb.MessageID{
							VertexName: "from-vertex",
						},
					},
				},
			},
			WriteMessages: []*isb.WriteMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							Headers: map[string]string{
								dfv1.KeyMetaID: "XXXX",
							},
						},
					},
				},
			},
		},
	}

	err := cp.NonSinkVertexCallback(ctx, messagePairs)
	assert.Nil(t, err)
}

func TestSinkVertexCallback(t *testing.T) {
	expected := []map[string]interface{}{
		{
			"vertex":      "testVertex",
			"pipeline":    "testPipeline",
			"id":          "XXXX",
			"from_vertex": "from-vertex",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// get request body and convert it to the text
		bodyBytes, _ := io.ReadAll(req.Body)
		var actual []map[string]interface{}
		err := json.Unmarshal(bodyBytes, &actual)
		assert.Nil(t, err)

		// remove the cb_time field
		for _, item := range actual {
			delete(item, "cb_time")
		}

		// compare the expected and actual body
		assert.Equal(t, expected, actual)

		// send 204 response
		rw.WriteHeader(http.StatusNoContent)
	}))

	defer server.Close()

	ctx := context.Background()
	cp := NewPublisher(ctx, "testVertex", "testPipeline", WithCallbackURL(server.URL))

	messages := []isb.Message{
		{
			Header: isb.Header{
				Headers: map[string]string{
					dfv1.KeyMetaCallbackURL: server.URL,
					dfv1.KeyMetaID:          "XXXX",
				},
				ID: isb.MessageID{
					VertexName: "from-vertex",
				},
			},
		},
	}

	err := cp.SinkVertexCallback(ctx, messages)
	assert.Nil(t, err)
}

func TestSinkVertexCallback_CallbackURLSet(t *testing.T) {
	expected := []map[string]interface{}{
		{
			"vertex":      "testVertex",
			"pipeline":    "testPipeline",
			"id":          "XXXX",
			"from_vertex": "from-vertex",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		bodyBytes, _ := io.ReadAll(req.Body)
		var actual []map[string]interface{}
		err := json.Unmarshal(bodyBytes, &actual)
		assert.Nil(t, err)

		for _, item := range actual {
			delete(item, "cb_time")
		}

		assert.Equal(t, expected, actual)
		rw.WriteHeader(http.StatusNoContent)
	}))

	defer server.Close()

	ctx := context.Background()
	cp := NewPublisher(ctx, "testVertex", "testPipeline", WithCallbackURL(server.URL))

	messages := []isb.Message{
		{
			Header: isb.Header{
				Headers: map[string]string{
					dfv1.KeyMetaID: "XXXX",
				},
				ID: isb.MessageID{
					VertexName: "from-vertex",
				},
			},
		},
	}

	err := cp.SinkVertexCallback(ctx, messages)
	assert.Nil(t, err)
}

func TestSinkVertexCallback_NoCallbackURL(t *testing.T) {
	ctx := context.Background()
	cp := NewPublisher(ctx, "testVertex", "testPipeline")

	messages := []isb.Message{
		{
			Header: isb.Header{
				Headers: map[string]string{
					dfv1.KeyMetaID: "XXXX",
				},
				ID: isb.MessageID{
					VertexName: "from-vertex",
				},
			},
		},
	}

	err := cp.SinkVertexCallback(ctx, messages)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "callback URL not found in headers and default callback URL is not set")
}
