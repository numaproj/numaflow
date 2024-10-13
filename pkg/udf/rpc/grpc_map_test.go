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

package rpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"github.com/numaproj/numaflow-go/pkg/mapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/numaproj/numaflow/pkg/isb"
	mapper2 "github.com/numaproj/numaflow/pkg/sdkclient/mapper"
)

func TestGRPCBasedMap_WaitUntilReadyWithServer(t *testing.T) {
	svc := &mapper.Service{
		Mapper: mapper.MapperFunc(func(ctx context.Context, keys []string, d mapper.Datum) mapper.Messages {
			return mapper.Messages{}
		}),
	}

	conn := newServer(t, func(server *grpc.Server) {
		mappb.RegisterMapServer(server, svc)
	})
	mapClient := mappb.NewMapClient(conn)
	client, _ := mapper2.NewFromClient(context.Background(), mapClient)
	u := NewUDSgRPCBasedMap(context.Background(), client, "testVertex")
	err := u.WaitUntilReady(context.Background())
	assert.NoError(t, err)
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

func TestGRPCBasedMap_ApplyMapWithServer(t *testing.T) {
	t.Run("test success", func(t *testing.T) {
		svc := &mapper.Service{
			Mapper: mapper.MapperFunc(func(ctx context.Context, keys []string, d mapper.Datum) mapper.Messages {
				return mapper.MessagesBuilder().Append(mapper.NewMessage(d.Value()).WithKeys(keys))
			}),
		}

		conn := newServer(t, func(server *grpc.Server) {
			mappb.RegisterMapServer(server, svc)
		})
		mapClient := mappb.NewMapClient(conn)
		ctx := context.Background()
		client, err := mapper2.NewFromClient(ctx, mapClient)
		require.NoError(t, err, "creating map client")
		u := NewUDSgRPCBasedMap(ctx, client, "testVertex")

		got, err := u.ApplyMap(ctx, []*isb.ReadMessage{{
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
		}})
		assert.NoError(t, err)
		assert.Equal(t, []string{"test_success_key"}, got[0].WriteMessages[0].Keys)
		assert.Equal(t, []byte(`forward_message`), got[0].WriteMessages[0].Payload)
	})

	t.Run("test error", func(t *testing.T) {
		svc := &mapper.Service{
			Mapper: mapper.MapperFunc(func(ctx context.Context, keys []string, d mapper.Datum) mapper.Messages {
				return mapper.Messages{}
			}),
		}

		conn := newServer(t, func(server *grpc.Server) {
			mappb.RegisterMapServer(server, svc)
		})
		mapClient := mappb.NewMapClient(conn)
		ctx, cancel := context.WithCancel(context.Background())
		client, err := mapper2.NewFromClient(ctx, mapClient)
		require.NoError(t, err, "creating map client")
		u := NewUDSgRPCBasedMap(ctx, client, "testVertex")

		// This cancelled context is passed to the ApplyMap function to simulate failure
		cancel()

		_, err = u.ApplyMap(ctx, []*isb.ReadMessage{{
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
		}})

		expectedUDFErr := &ApplyUDFErr{
			UserUDFErr: false,
			Message:    "gRPC client.MapFn failed, context canceled",
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
		var receivedErr *ApplyUDFErr
		assert.ErrorAs(t, err, &receivedErr)
		assert.Equal(t, expectedUDFErr, receivedErr)
	})
}
