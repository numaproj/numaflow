package function

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
	// 204
	req := httptest.NewRequest(http.MethodGet, "/messages", nil)
	w := httptest.NewRecorder()
	ctx := context.Background()
	udf(ctx, w, req, dummyTestHandler, contentTypeMsgPack)
	res := w.Result()
	defer func() { _ = res.Body.Close() }()
	_, err := ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)

	// 200
	input := bytes.NewBufferString("hello")
	req = httptest.NewRequest(http.MethodPost, "/messages", input)
	w = httptest.NewRecorder()
	udf(ctx, w, req, dummyTestHandler, contentTypeMsgPack)
	res = w.Result()
	defer func() { _ = res.Body.Close() }()
	assert.Equal(t, 200, res.StatusCode)
	var data []byte
	data, err = ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	messages := []Message{}
	err = msgpack.Unmarshal(data, &messages)
	assert.NoError(t, err)
	assert.Equal(t, bytes.NewBufferString("hello").Bytes(), messages[0].Key)
	assert.Equal(t, bytes.NewBufferString("hello").Bytes(), messages[0].Value)

	// 200 with nil key
	input = bytes.NewBufferString("no_key")
	req = httptest.NewRequest(http.MethodPost, "/messages", input)
	w = httptest.NewRecorder()
	udf(ctx, w, req, func(ctx context.Context, key, msg []byte) (Messages, error) {
		return MessagesBuilder().Append(MessageTo("", msg)), nil
	}, contentTypeMsgPack)
	res = w.Result()
	defer func() { _ = res.Body.Close() }()

	data, err = ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)
	messages = []Message{}
	err = msgpack.Unmarshal(data, &messages)
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, messages[0].Key)
	assert.Equal(t, bytes.NewBufferString("no_key").Bytes(), messages[0].Value)

	// 50X
	req = httptest.NewRequest(http.MethodPost, "/messages", nil)
	w = httptest.NewRecorder()
	udf(ctx, w, req, func(ctx context.Context, key, msg []byte) (Messages, error) {
		return nil, fmt.Errorf("test error")
	}, contentTypeMsgPack)
	res = w.Result()
	defer func() { _ = res.Body.Close() }()
	_, err = ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode)
}

func dummyTestHandler(_ context.Context, key, m []byte) (messages Messages, error error) {
	if len(m) == 0 {
		return nil, nil
	}
	return MessagesBuilder().Append(Message{Key: m, Value: m}), nil
}
