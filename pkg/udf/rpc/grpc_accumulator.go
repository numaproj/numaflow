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
	"fmt"
	"time"

	accumulatorpb "github.com/numaproj/numaflow-go/pkg/apis/proto/accumulator/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/sdkclient/accumulator"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/window"
)

// GRPCBasedAccumulator is a reduce applier that uses gRPC client to invoke the accumulator UDF. It implements the applier.ReduceApplier interface.
type GRPCBasedAccumulator struct {
	vertexName string
	client     accumulator.Client
}

func NewGRPCBasedAccumulator(vertexName string, client accumulator.Client) *GRPCBasedAccumulator {
	return &GRPCBasedAccumulator{
		vertexName: vertexName,
		client:     client,
	}
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedAccumulator) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedAccumulator) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the reduce udf is connected.
func (u *GRPCBasedAccumulator) WaitUntilReady(ctx context.Context) error {
	log := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				log.Infof("waiting for reduce udf to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// ApplyReduce accepts a channel of timedWindowRequest and returns the result in a channel of timedWindowResponse
func (u *GRPCBasedAccumulator) ApplyReduce(ctx context.Context, _ *partition.ID, requestsStream <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *window.TimedWindowResponse)
		requestCh  = make(chan *accumulatorpb.AccumulatorRequest)
	)

	// invoke the AsyncReduceFn method with requestCh channel and send the result to responseCh channel
	// and any error to errCh channel
	go func() {
		resultCh, reduceErrCh := u.client.AccumulatorFn(ctx, requestCh)
		for {
			select {
			case result, ok := <-resultCh:
				if !ok || result == nil {
					errCh = nil
					// if the resultCh channel is closed, close the responseCh and errCh channels and return
					close(responseCh)
					return
				}
				// generate the unique ID for the window to keep track of the response count for the window using the resultsMap
				responseCh <- u.parseAccumulatorResponse(result)
			case err := <-reduceErrCh:
				// ctx.Done() event will be handled by the AsyncReduceFn method
				// so we don't need a separate case for ctx.Done() here
				if errors.Is(err, ctx.Err()) {
					errCh <- err
					return
				}
				if err != nil {
					errCh <- convertToUdfError(err)
				}
			}
		}
	}()

	// create AccumulatorRequest from TimedWindowRequest and send it to requestCh channel for AsyncReduceFn
	go func() {
		// after reading all the requests from the requestsStream or if ctx was canceled close the requestCh channel
		defer func() {
			close(requestCh)
		}()
		for {
			select {
			case req, ok := <-requestsStream:
				// if the requestsStream is closed or if the request is nil, return
				if !ok || req == nil {
					return
				}

				d := createAccumulatorRequest(req)

				// send the request to requestCh channel, handle the case when the context is canceled
				select {
				case <-ctx.Done():
					return
				case requestCh <- d:
				}

			case <-ctx.Done(): // if the context is done, return
				return
			}
		}
	}()

	return responseCh, errCh
}

func createAccumulatorRequest(windowRequest *window.TimedWindowRequest) *accumulatorpb.AccumulatorRequest {
	var payload = &accumulatorpb.Payload{}
	if windowRequest.ReadMessage != nil {
		payload = &accumulatorpb.Payload{
			Keys:      windowRequest.ReadMessage.Keys,
			Value:     windowRequest.ReadMessage.Payload,
			EventTime: timestamppb.New(windowRequest.ReadMessage.MessageInfo.EventTime),
			Watermark: timestamppb.New(windowRequest.ReadMessage.Watermark),
			Headers:   windowRequest.ReadMessage.Headers,
			Id:        windowRequest.ReadMessage.ID.String(),
		}
	}

	var windowOp accumulatorpb.AccumulatorRequest_WindowOperation_Event
	switch windowRequest.Operation {
	case window.Open:
		windowOp = accumulatorpb.AccumulatorRequest_WindowOperation_OPEN
	case window.Append:
		windowOp = accumulatorpb.AccumulatorRequest_WindowOperation_APPEND
	case window.Close:
		windowOp = accumulatorpb.AccumulatorRequest_WindowOperation_CLOSE
	default:
		panic("unhandled default case")
	}

	// for accumulator we will have only one window
	w := windowRequest.Windows[0]
	keyedWindow := &accumulatorpb.KeyedWindow{
		Start: timestamppb.New(w.StartTime()),
		End:   timestamppb.New(w.EndTime()),
		Slot:  w.Slot(),
		Keys:  w.Keys(),
	}

	var d = &accumulatorpb.AccumulatorRequest{
		Payload: payload,
		Operation: &accumulatorpb.AccumulatorRequest_WindowOperation{
			Event:       windowOp,
			KeyedWindow: keyedWindow,
		},
	}
	return d
}

func (u *GRPCBasedAccumulator) parseAccumulatorResponse(response *accumulatorpb.AccumulatorResponse) *window.TimedWindowResponse {
	start := response.GetWindow().GetStart().AsTime()
	end := response.GetWindow().GetEnd().AsTime()
	slot := response.GetWindow().GetSlot()
	keys := response.GetWindow().GetKeys()

	var taggedMessage *isb.WriteMessage
	if result := response.GetPayload(); result != nil {
		taggedMessage = &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					ID: isb.MessageID{
						VertexName: u.vertexName,
						Offset:     result.GetId(),
						Index:      0,
					},
					MessageInfo: isb.MessageInfo{
						EventTime: result.GetEventTime().AsTime(),
						IsLate:    false,
					},
					Keys: result.GetKeys(),
				},
				Body: isb.Body{
					Payload: result.GetValue(),
				},
			},
		}
	}

	return &window.TimedWindowResponse{
		WriteMessage: taggedMessage,
		Window:       window.NewUnalignedTimedWindow(start, end, slot, keys),
	}
}
