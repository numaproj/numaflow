package applier

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow-go/pkg/function/client"
	"github.com/numaproj/numaflow-go/pkg/function/server"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func handle(_ context.Context, key string, msg []byte) (functionsdk.Messages, error) {
	return functionsdk.MessagesBuilder().Append(functionsdk.MessageTo(key, msg)), nil
}

func TestGRPCBasedUDF_WaitUntilReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	go server.NewServer().RegisterMapper(functionsdk.DoFunc(handle)).Start()

	u, err := NewUDSGRPCBasedUDF(ctx)
	assert.Nil(t, err)
	err = u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func NewMockUDSGRPCBasedUDF(mockClient *funcmock.MockUserDefinedFunctionClient) *UDSGRPCBasedUDF {
	c, _ := client.NewClient(client.WithMockGRPCClient(mockClient))
	return &UDSGRPCBasedUDF{c}
}

func TestGRPCBasedUDF_WaitUntilReadyWithMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&functionpb.ReadyResponse{Ready: true}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	u := NewMockUDSGRPCBasedUDF(mockClient)
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func TestGRPCBasedUDF_BasicApply(t *testing.T) {
	t.Run("test 200", func(t *testing.T) {
		u := NewUDSHTTPBasedUDF(testSocketPath)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// 200
		ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", string(dfv1.MsgPackType))
			w.WriteHeader(http.StatusOK)
			msgs := functionsdk.MessagesBuilder().Append(functionsdk.Message{Key: "", Value: []byte("test")})
			b, _ := msgpack.Marshal(msgs)
			_, _ = w.Write(b)
		}))
		_ = os.Remove(testSocketPath)
		listener, _ := net.Listen("unix", testSocketPath)
		defer func() { _ = listener.Close() }()
		ts.Listener = listener
		ts.Start()
		u.client = testClient
		_, err := u.Apply(ctx, &isb.ReadMessage{ReadOffset: isb.SimpleOffset(func() string { return "" })})
		assert.NoError(t, err)
		ts.Close()
	})

	t.Run("test 503", func(t *testing.T) {
		u := NewUDSHTTPBasedUDF(testSocketPath)
		ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintln(w, "test")
		}))
		_ = os.Remove(testSocketPath)
		listener, _ := net.Listen("unix", testSocketPath)
		defer func() { _ = listener.Close() }()
		ts.Listener = listener
		ts.Start()
		u.client = testClient
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := u.Apply(ctx, &isb.ReadMessage{})
		assert.Error(t, err)
		ts.Close()
	})
}

func TestHGRPCBasedUDF_Apply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	multiplyBy2 := func(body []byte) interface{} {
		var result testutils.PayloadForTest
		_ = json.Unmarshal(body, &result)
		result.Value = result.Value * 2
		return result
	}

	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)

		// set header as even or odd based on the original request
		var orignalReq testutils.PayloadForTest
		_ = json.Unmarshal(body, &orignalReq)
		result, _ := json.Marshal(multiplyBy2(body).(testutils.PayloadForTest))
		m := &functionsdk.Message{Value: result}
		if orignalReq.Value%2 == 0 {
			m.Key = "even"
		} else {
			m.Key = "odd"
		}
		b, _ := msgpack.Marshal(functionsdk.MessagesBuilder().Append(*m))
		w.Header().Add("Content-Type", string(dfv1.MsgPackType))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)

		_, _ = w.Write(result)
	}))
	_ = os.Remove(testSocketPath)
	listener, _ := net.Listen("unix", testSocketPath)
	defer func() { _ = listener.Close() }()
	s.Listener = listener
	s.Start()
	defer s.Close()

	u := NewUDSHTTPBasedUDF(testSocketPath)
	u.client = testClient
	var count = int64(10)
	readMessages := testutils.BuildTestReadMessages(count, time.Unix(1636470000, 0))
	var expectedResults = make([][]byte, count)
	var expectedKeys = make([]string, count)
	for idx, readMessage := range readMessages {
		var readMessagePayload testutils.PayloadForTest
		_ = json.Unmarshal(readMessage.Payload, &readMessagePayload)
		if readMessagePayload.Value%2 == 0 {
			expectedKeys[idx] = "even"
		} else {
			expectedKeys[idx] = "odd"
		}
		marshal, _ := json.Marshal(multiplyBy2(readMessage.Payload))
		expectedResults[idx] = marshal
	}

	var results = make([][]byte, len(readMessages))
	var resultKeys = make([]string, len(readMessages))
	for idx, readMessage := range readMessages {
		apply, err := u.Apply(ctx, &readMessage)
		assert.NoError(t, err)
		results[idx] = apply[0].Payload
		resultKeys[idx] = string(apply[0].Header.Key)
	}

	assert.Equal(t, expectedResults, results)
	assert.Equal(t, expectedKeys, resultKeys)
}
