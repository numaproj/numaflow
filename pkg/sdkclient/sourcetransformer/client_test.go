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

package sourcetransformer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	transformpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1"
	"github.com/numaproj/numaflow-go/pkg/sourcetransformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClient_IsReady(t *testing.T) {
	var ctx = context.Background()
	svc := &sourcetransformer.Service{
		Transformer: sourcetransformer.SourceTransformFunc(func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
			return sourcetransformer.MessagesBuilder()
		}),
	}

	// Start the gRPC server
	conn := newServer(t, func(server *grpc.Server) {
		transformpb.RegisterSourceTransformServer(server, svc)
	})
	defer conn.Close()

	// Create a client connection to the server
	client := transformpb.NewSourceTransformClient(conn)

	testClient, err := NewFromClient(ctx, client)
	require.NoError(t, err)

	ready, err := testClient.IsReady(ctx, &emptypb.Empty{})
	require.True(t, ready)
	require.NoError(t, err)
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

func TestClient_SourceTransformFn(t *testing.T) {
	var testTime = time.Date(2021, 8, 15, 14, 30, 45, 100, time.Local)
	svc := &sourcetransformer.Service{
		Transformer: sourcetransformer.SourceTransformFunc(func(ctx context.Context, keys []string, datum sourcetransformer.Datum) sourcetransformer.Messages {
			msg := datum.Value()
			return sourcetransformer.MessagesBuilder().Append(sourcetransformer.NewMessage(msg, testTime).WithKeys([]string{keys[0] + "_test"}))
		}),
	}
	conn := newServer(t, func(server *grpc.Server) {
		transformpb.RegisterSourceTransformServer(server, svc)
	})
	transformClient := transformpb.NewSourceTransformClient(conn)
	var ctx = context.Background()
	client, _ := NewFromClient(ctx, transformClient)

	requests := make([]*transformpb.SourceTransformRequest, 5)
	go func() {
		for i := 0; i < 5; i++ {
			requests[i] = &transformpb.SourceTransformRequest{
				Request: &transformpb.SourceTransformRequest_Request{
					Keys:  []string{fmt.Sprintf("client_key_%d", i)},
					Value: []byte("test"),
				},
			}
		}
	}()

	responses, err := client.SourceTransformFn(ctx, requests)
	require.NoError(t, err)
	var results [][]*transformpb.SourceTransformResponse_Result
	for _, resp := range responses {
		results = append(results, resp.GetResults())
	}
	expected := [][]*transformpb.SourceTransformResponse_Result{
		{{Keys: []string{"client_key_0_test"}, Value: []byte("test"), EventTime: timestamppb.New(testTime)}},
		{{Keys: []string{"client_key_1_test"}, Value: []byte("test"), EventTime: timestamppb.New(testTime)}},
		{{Keys: []string{"client_key_2_test"}, Value: []byte("test"), EventTime: timestamppb.New(testTime)}},
		{{Keys: []string{"client_key_3_test"}, Value: []byte("test"), EventTime: timestamppb.New(testTime)}},
		{{Keys: []string{"client_key_4_test"}, Value: []byte("test"), EventTime: timestamppb.New(testTime)}},
	}
	require.ElementsMatch(t, expected, results)
}
