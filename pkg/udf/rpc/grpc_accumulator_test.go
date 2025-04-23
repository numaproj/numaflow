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
	"testing"
	"time"

	"github.com/numaproj/numaflow-go/pkg/accumulator"
	accumulatorpb "github.com/numaproj/numaflow-go/pkg/apis/proto/accumulator/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	client "github.com/numaproj/numaflow/pkg/sdkclient/accumulator"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/window"
)

func TestGRPCBasedAccumulator_IsHealthy(t *testing.T) {
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
	ctx := context.Background()
	testClient, _ := client.NewFromClient(ctx, accumulatorClient)
	u := NewGRPCBasedAccumulator("testVertex", testClient)

	err := u.IsHealthy(ctx)
	assert.NoError(t, err)
}

func TestGRPCBasedAccumulator_ApplyReduce(t *testing.T) {
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
	ctx := context.Background()
	testClient, _ := client.NewFromClient(ctx, accumulatorClient)
	u := NewGRPCBasedAccumulator("testVertex", testClient)

	requestsStream := make(chan *window.TimedWindowRequest, 5)
	for i := 0; i < 5; i++ {
		requestsStream <- &window.TimedWindowRequest{
			ReadMessage: &isb.ReadMessage{
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
						Payload: []byte(`test-accumulator`),
					},
				},
				ReadOffset: isb.SimpleStringOffset(func() string { return "0" }),
			},
			Operation: window.Append,
			Windows:   []window.TimedWindow{window.NewUnalignedTimedWindow(time.Now(), time.Now(), "slot-0", []string{"test_success_key"})},
		}
	}
	close(requestsStream)

	responseCh, errCh := u.ApplyReduce(ctx, &partition.ID{}, requestsStream)

	expectedValue := []byte("test-accumulator")
	i := 0
	for resp := range responseCh {
		assert.Equal(t, expectedValue, resp.WriteMessage.Payload)
		i++
	}

	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}
