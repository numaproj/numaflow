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

package accumulator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/numaproj/numaflow-go/pkg/accumulator"
	accumulatorpb "github.com/numaproj/numaflow-go/pkg/apis/proto/accumulator/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

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

func TestClient_AccumulatorFn(t *testing.T) {
	svc := &accumulator.Service{
		AccumulatorCreator: accumulator.SimpleCreatorWithAccumulateFn(func(ctx context.Context, input <-chan accumulator.Datum, output chan<- accumulator.Message) {
			for datum := range input {
				output <- accumulator.MessageFromDatum(datum)
			}
		}),
	}

	conn := newServer(t, func(server *grpc.Server) {
		accumulatorpb.RegisterAccumulatorServer(server, svc)
	})
	accumulatorClient := accumulatorpb.NewAccumulatorClient(conn)
	var ctx = context.Background()
	testClient, _ := NewFromClient(ctx, accumulatorClient)

	ready, err := testClient.IsReady(ctx, &emptypb.Empty{})
	require.True(t, ready)
	require.NoError(t, err)

	requests := make([]*accumulatorpb.AccumulatorRequest, 5)
	for i := 0; i < 5; i++ {
		requests[i] = &accumulatorpb.AccumulatorRequest{
			Payload: &accumulatorpb.Payload{
				Keys:  []string{fmt.Sprintf("client_key_%d", i)},
				Value: []byte("test"),
			},
			Operation: &accumulatorpb.AccumulatorRequest_WindowOperation{
				Event: accumulatorpb.AccumulatorRequest_WindowOperation_APPEND,
			},
		}
	}

	datumStreamCh := make(chan *accumulatorpb.AccumulatorRequest, len(requests))
	for _, req := range requests {
		datumStreamCh <- req
	}
	close(datumStreamCh)

	responseCh, errCh := testClient.AccumulatorFn(ctx, datumStreamCh)
	expectedValue := []byte("test")

	i := 0
	for resp := range responseCh {
		assert.Equal(t, expectedValue, resp.GetPayload().GetValue())
		i++
	}

	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}
