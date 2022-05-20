package sink

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestStart_simpleStop(t *testing.T) {
	t.SkipNow()
	// start and stop in a second
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// start at random port and block till timeout
	Start(ctx, dummyTestHandler, WithDrainTimeout(time.Minute))
}

func TestStart_testReady(t *testing.T) {
	// 200
	ctx := context.Background()
	msgs := []Message{{ID: "abc", Payload: []byte("message")}}
	b, err := msgpack.Marshal(msgs)
	assert.NoError(t, err)
	input := bytes.NewBuffer(b)
	req := httptest.NewRequest(http.MethodPost, "/messages", input)
	w := httptest.NewRecorder()
	udsink(ctx, w, req, dummyTestHandler)
	res := w.Result()
	defer func() { _ = res.Body.Close() }()
	assert.Equal(t, 200, res.StatusCode)
	var data []byte
	data, err = ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	responses := Responses{}
	err = msgpack.Unmarshal(data, &responses)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(responses.Items()))
	assert.Equal(t, "abc", responses.Items()[0].ID)
	assert.True(t, responses.Items()[0].Success)

	// 50X
	req = httptest.NewRequest(http.MethodPost, "/messages", nil)
	w = httptest.NewRecorder()
	udsink(ctx, w, req, func(ctx context.Context, msgs []Message) (Responses, error) {
		return nil, fmt.Errorf("test error")
	})
	res = w.Result()
	defer func() { _ = res.Body.Close() }()
	_, err = ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode)
}

func dummyTestHandler(_ context.Context, msgs []Message) (Responses, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	return ResponsesBuilder().Append(ResponseOK("abc")), nil
}
