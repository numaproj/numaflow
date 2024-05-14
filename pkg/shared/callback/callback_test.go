package callback

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
)

func TestEndToEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// get request body and convert it to the text
		bodyBytes, _ := ioutil.ReadAll(req.Body)
		bodyString := string(bodyBytes)
		println(bodyString)
		// send 204 response
		rw.WriteHeader(http.StatusNoContent)
	}))

	defer server.Close()

	ctx := context.Background()
	cp := NewPublisher(ctx, "testVertex", "testPipeline", WithCallbackURL(server.URL), WithCallbackHeaderKey("cb_url"))

	messagePairs := []isb.ReadWriteMessagePair{
		{
			ReadMessage: &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{
						Headers: map[string]string{
							"cb_url": server.URL,
							"uuid":   "XXXX",
						},
					},
				},
			},
			WriteMessages: []*isb.WriteMessage{
				{
					Message: isb.Message{
						Header: isb.Header{
							Headers: map[string]string{
								"cb_url": server.URL,
								"uuid":   "XXXX",
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
