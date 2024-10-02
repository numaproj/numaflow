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

package transformer

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/numaproj/numaflow-go/pkg/sourcetransformer"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	transformpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	sourcetransformerSdk "github.com/numaproj/numaflow/pkg/sdkclient/sourcetransformer"
	"github.com/numaproj/numaflow/pkg/udf/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGRPCBasedTransformer_WaitUntilReadyWithServer(t *testing.T) {
	svc := &sourcetransformer.Service{
		Transformer: sourcetransformer.SourceTransformFunc(func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
			return sourcetransformer.Messages{}
		}),
	}

	conn := newServer(t, func(server *grpc.Server) {
		transformpb.RegisterSourceTransformServer(server, svc)
	})
	transformClient := transformpb.NewSourceTransformClient(conn)
	client, _ := sourcetransformerSdk.NewFromClient(context.Background(), transformClient)
	u := NewGRPCBasedTransformer("testVertex", client)
	err := u.WaitUntilReady(context.Background())
	assert.NoError(t, err)
}

func TestGRPCBasedTransformer_BasicApplyWithServer(t *testing.T) {
	t.Run("test success", func(t *testing.T) {
		svc := &sourcetransformer.Service{
			Transformer: sourcetransformer.SourceTransformFunc(func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
				return sourcetransformer.MessagesBuilder().Append(sourcetransformer.NewMessage(datum.Value(), datum.EventTime()).WithKeys(keys))
			}),
		}

		conn := newServer(t, func(server *grpc.Server) {
			transformpb.RegisterSourceTransformServer(server, svc)
		})
		transformClient := transformpb.NewSourceTransformClient(conn)
		ctx := context.Background()
		client, err := sourcetransformerSdk.NewFromClient(ctx, transformClient)
		require.NoError(t, err, "creating source transformer client")
		u := NewGRPCBasedTransformer("testVertex", client)

		got, err := u.ApplyTransform(ctx, []*isb.ReadMessage{{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169600, 0),
					},
					ID: isb.MessageID{
						VertexName: "test-vertex",
						Offset:     "0-0",
					},
					Keys: []string{"test_success_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		}},
		)
		assert.NoError(t, err)
		assert.Equal(t, []string{"test_success_key"}, got[0].WriteMessages[0].Keys)
		assert.Equal(t, []byte(`forward_message`), got[0].WriteMessages[0].Payload)
	})

	t.Run("test error", func(t *testing.T) {
		svc := &sourcetransformer.Service{
			Transformer: sourcetransformer.SourceTransformFunc(func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
				return sourcetransformer.Messages{}
			}),
		}

		conn := newServer(t, func(server *grpc.Server) {
			transformpb.RegisterSourceTransformServer(server, svc)
		})
		transformClient := transformpb.NewSourceTransformClient(conn)
		ctx, cancel := context.WithCancel(context.Background())
		client, err := sourcetransformerSdk.NewFromClient(ctx, transformClient)
		require.NoError(t, err, "creating source transformer client")
		u := NewGRPCBasedTransformer("testVertex", client)

		// This cancelled context is passed to the ApplyTransform function to simulate failure
		cancel()

		_, err = u.ApplyTransform(ctx, []*isb.ReadMessage{{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: time.Unix(1661169660, 0),
					},
					ID: isb.MessageID{
						VertexName: "test-vertex",
						Offset:     "0-0",
					},
					Keys: []string{"test_error_key"},
				},
				Body: isb.Body{
					Payload: []byte(`forward_message`),
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
		}},
		)

		expectedUDFErr := &rpc.ApplyUDFErr{
			UserUDFErr: false,
			Message:    "gRPC client.SourceTransformFn failed, context canceled",
			InternalErr: rpc.InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
		var receivedErr *rpc.ApplyUDFErr
		assert.ErrorAs(t, err, &receivedErr)
		assert.Equal(t, expectedUDFErr, receivedErr)
	})
}

func TestGRPCBasedTransformer_ApplyWithServer_ChangePayload(t *testing.T) {
	svc := &sourcetransformer.Service{
		Transformer: sourcetransformer.SourceTransformFunc(func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
			var originalValue testutils.PayloadForTest
			_ = json.Unmarshal(datum.Value(), &originalValue)
			doubledValue := testutils.PayloadForTest{
				Value: originalValue.Value * 2,
				Key:   originalValue.Key,
			}
			doubledValueBytes, _ := json.Marshal(&doubledValue)

			var resultKeys []string
			if originalValue.Value%2 == 0 {
				resultKeys = []string{"even"}
			} else {
				resultKeys = []string{"odd"}
			}
			return sourcetransformer.MessagesBuilder().Append(sourcetransformer.NewMessage(doubledValueBytes, datum.EventTime()).WithKeys(resultKeys))
		}),
	}

	conn := newServer(t, func(server *grpc.Server) {
		transformpb.RegisterSourceTransformServer(server, svc)
	})
	transformClient := transformpb.NewSourceTransformClient(conn)
	ctx := context.Background()
	client, _ := sourcetransformerSdk.NewFromClient(ctx, transformClient)
	u := NewGRPCBasedTransformer("testVertex", client)

	var count = int64(10)
	readMessages := testutils.BuildTestReadMessages(count, time.Unix(1661169600, 0), nil)
	messages := make([]*isb.ReadMessage, len(readMessages))
	for idx, readMessage := range readMessages {
		messages[idx] = &readMessage
	}
	apply, err := u.ApplyTransform(context.TODO(), messages)
	assert.NoError(t, err)

	for _, pair := range apply {
		resultPayload := pair.WriteMessages[0].Payload
		resultKeys := pair.WriteMessages[0].Header.Keys
		var readMessagePayload testutils.PayloadForTest
		_ = json.Unmarshal(pair.ReadMessage.Payload, &readMessagePayload)
		var expectedKeys []string
		if readMessagePayload.Value%2 == 0 {
			expectedKeys = []string{"even"}
		} else {
			expectedKeys = []string{"odd"}
		}
		assert.Equal(t, expectedKeys, resultKeys)

		doubledValue := testutils.PayloadForTest{
			Key:   readMessagePayload.Key,
			Value: readMessagePayload.Value * 2,
		}
		marshal, _ := json.Marshal(doubledValue)
		assert.Equal(t, marshal, resultPayload)
	}
}

func newServer(t *testing.T, register func(server *grpc.Server)) *grpc.ClientConn {
	lis := bufconn.Listen(100)
	t.Cleanup(func() {
		_ = lis.Close()
	})

	server := grpc.NewServer()
	t.Cleanup(func() {
		server.Stop()
	})

	register(server)

	errChan := make(chan error, 1)
	go func() {
		// t.Fatal should only be called from the goroutine running the test
		if err := server.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(func() {
		_ = conn.Close()
	})
	if err != nil {
		t.Fatalf("Creating new gRPC client connection: %v", err)
	}

	var grpcServerErr error
	select {
	case grpcServerErr = <-errChan:
	case <-time.After(500 * time.Millisecond):
		grpcServerErr = errors.New("gRPC server didn't start in 500ms")
	}
	if err != nil {
		t.Fatalf("Failed to start gRPC server: %v", grpcServerErr)
	}

	return conn
}

func TestGRPCBasedTransformer_Apply_ChangeEventTime(t *testing.T) {
	testEventTime := time.Date(1992, 2, 8, 0, 0, 0, 100, time.UTC)
	svc := &sourcetransformer.Service{
		Transformer: sourcetransformer.SourceTransformFunc(func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
			msg := datum.Value()
			return sourcetransformer.MessagesBuilder().Append(sourcetransformer.NewMessage(msg, testEventTime).WithKeys([]string{"even"}))
		}),
	}
	conn := newServer(t, func(server *grpc.Server) {
		transformpb.RegisterSourceTransformServer(server, svc)
	})
	transformClient := transformpb.NewSourceTransformClient(conn)
	ctx := context.Background()
	client, _ := sourcetransformerSdk.NewFromClient(ctx, transformClient)
	u := NewGRPCBasedTransformer("testVertex", client)

	var count = int64(2)
	readMessages := testutils.BuildTestReadMessages(count, time.Unix(1661169600, 0), nil)
	messages := make([]*isb.ReadMessage, len(readMessages))
	for idx, readMessage := range readMessages {
		messages[idx] = &readMessage
	}
	apply, err := u.ApplyTransform(context.TODO(), messages)
	assert.NoError(t, err)
	for _, pair := range apply {
		assert.NoError(t, pair.Err)
		assert.Equal(t, testEventTime, pair.WriteMessages[0].EventTime)
	}
}
