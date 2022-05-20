package udsink

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	sinksdk "github.com/numaproj/numaflow/sdks/golang/sink"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	testSocketPath = "/tmp/test-sink.sock"
)

var (
	testTransport = &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", testSocketPath)
		},
	}

	testClient = &http.Client{
		Transport: testTransport,
	}
)

func TestHTTPBasedUDSink_WaitUntilReady(t *testing.T) {
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "Hello, client")
	}))
	_ = os.Remove(testSocketPath)
	listener, _ := net.Listen("unix", testSocketPath)
	defer func() { _ = listener.Close() }()
	ts.Listener = listener
	ts.Start()
	defer ts.Close()

	u := NewUDSHTTPBasedUDSink(testSocketPath)
	u.client = testClient
	ctx := context.Background()
	err := u.WaitUntilReady(ctx)
	assert.NoError(t, err)
}

func TestHTTPBasedUDSink_BasicApply(t *testing.T) {
	t.Run("test 200", func(t *testing.T) {
		u := NewUDSHTTPBasedUDSink(testSocketPath)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// 200
		ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", string(dfv1.MsgPackType))
			w.WriteHeader(http.StatusOK)
			responses := sinksdk.ResponsesBuilder().Append(sinksdk.ResponseOK("abc"))
			b, _ := msgpack.Marshal(responses)
			_, _ = w.Write(b)
		}))
		_ = os.Remove(testSocketPath)
		listener, _ := net.Listen("unix", testSocketPath)
		defer func() { _ = listener.Close() }()
		ts.Listener = listener
		ts.Start()
		u.client = testClient
		errs := u.Apply(ctx, []sinksdk.Message{{ID: "abc", Payload: []byte("hello")}})
		assert.Equal(t, 1, len(errs))
		assert.Nil(t, errs[0])
		ts.Close()
	})

	t.Run("test 503", func(t *testing.T) {
		u := NewUDSHTTPBasedUDSink(testSocketPath)
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
		errs := u.Apply(ctx, []sinksdk.Message{{ID: "abc", Payload: []byte("hello")}})
		assert.Equal(t, 1, len(errs))
		assert.NotNil(t, errs[0])
		ts.Close()
	})
}
