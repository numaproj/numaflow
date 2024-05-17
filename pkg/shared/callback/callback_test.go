package callback

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
)

func TestEndToEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// get request body and convert it to the text
		bodyBytes, _ := io.ReadAll(req.Body)
		bodyString := string(bodyBytes)
		println(bodyString)
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
