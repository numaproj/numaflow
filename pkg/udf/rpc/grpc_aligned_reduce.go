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
	"fmt"
	"strconv"
	"time"

	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	"github.com/numaproj/numaflow/pkg/sdkclient/reducer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/window"
)

// FIXME(session): rename file, type, NewXXX to Aligned

// GRPCBasedAlignedReduce is a reduce applier that uses gRPC client to invoke the aligned reduce UDF. It implements the applier.ReduceApplier interface.
type GRPCBasedAlignedReduce struct {
	vertexName    string
	vertexReplica int32
	client        reducer.Client
}

func NewUDSgRPCAlignedReduce(vertexName string, vertexReplica int32, client reducer.Client) *GRPCBasedAlignedReduce {
	return &GRPCBasedAlignedReduce{
		vertexName:    vertexName,
		vertexReplica: vertexReplica,
		client:        client,
	}
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedAlignedReduce) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedAlignedReduce) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the reduce udf is connected.
func (u *GRPCBasedAlignedReduce) WaitUntilReady(ctx context.Context) error {
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
func (u *GRPCBasedAlignedReduce) ApplyReduce(ctx context.Context, partitionID *partition.ID, requestsStream <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
	var (
		errCh          = make(chan error)
		responseCh     = make(chan *window.TimedWindowResponse)
		reduceRequests = make(chan *reducepb.ReduceRequest)
	)

	// pass key and window information inside the context
	mdMap := map[string]string{
		sdkclient.WinStartTime: strconv.FormatInt(partitionID.Start.UnixMilli(), 10),
		sdkclient.WinEndTime:   strconv.FormatInt(partitionID.End.UnixMilli(), 10),
	}

	grpcCtx := metadata.NewOutgoingContext(ctx, metadata.New(mdMap))

	index := 0
	// invoke the AsyncReduceFn method with reduceRequests channel and send the result to responseCh channel
	// and any error to errCh channel
	go func() {
		resultCh, reduceErrCh := u.client.ReduceFn(grpcCtx, reduceRequests)
		for {
			select {
			case result, ok := <-resultCh:
				if !ok || result == nil {
					errCh = nil
					// if the resultCh channel is closed, close the responseCh and return
					close(responseCh)
					return
				}
				// create a unique message id for each response message which will be used for deduplication
				msgId := fmt.Sprintf("%s-%d-%s-%d", u.vertexName, u.vertexReplica, partitionID.String(), index)
				index++
				responseCh <- parseReduceResponse(result, msgId)
			case err := <-reduceErrCh:
				// ctx.Done() event will be handled by the AsyncReduceFn method
				// so we don't need a separate case for ctx.Done() here
				if err == ctx.Err() {
					errCh <- err
					return
				}
				if err != nil {
					errCh <- convertToUdfError(err)
				}
			}
		}
	}()

	// create ReduceRequest from TimedWindowRequest and send it to reduceRequests channel for AsyncReduceFn
	go func() {
		// after reading all the messages from the requestsStream or if ctx was canceled close the reduceRequests channel
		defer func() {
			close(reduceRequests)
		}()
		for {
			select {
			case msg, ok := <-requestsStream:
				// if the requestsStream is closed or if the message is nil, return
				if !ok || msg == nil {
					return
				}

				d := createAlignedReduceRequest(msg)
				// send the datum to reduceRequests channel, handle the case when the context is canceled
				select {
				case reduceRequests <- d:
				case <-ctx.Done():
					return
				}

			case <-ctx.Done(): // if the context is done, don't send any more datum to reduceRequests channel
				return
			}
		}
	}()

	return responseCh, errCh
}

func createAlignedReduceRequest(windowRequest *window.TimedWindowRequest) *reducepb.ReduceRequest {
	var windowOp reducepb.ReduceRequest_WindowOperation_Event
	var windows []*reducepb.Window

	for _, w := range windowRequest.Windows {
		windows = append(windows, &reducepb.Window{
			Start: timestamppb.New(w.StartTime()),
			End:   timestamppb.New(w.EndTime()),
			Slot:  w.Slot(),
		})
	}
	// for aligned window event can be either open or append
	// since closing the pbq channel is like a close event for the window
	// when pbq channel is closed, grpc client stream will be closed and
	// server will consider the grpc client stream as closed event for the window
	switch windowRequest.Operation {
	case window.Open:
		windowOp = reducepb.ReduceRequest_WindowOperation_OPEN
	default:
		windowOp = reducepb.ReduceRequest_WindowOperation_APPEND
	}

	var payload = &reducepb.ReduceRequest_Payload{}
	if windowRequest.ReadMessage != nil {
		payload = &reducepb.ReduceRequest_Payload{
			Keys:      windowRequest.ReadMessage.Keys,
			Value:     windowRequest.ReadMessage.Payload,
			EventTime: timestamppb.New(windowRequest.ReadMessage.MessageInfo.EventTime),
			Watermark: timestamppb.New(windowRequest.ReadMessage.Watermark),
		}
	}

	var d = &reducepb.ReduceRequest{
		Payload: payload,
		Operation: &reducepb.ReduceRequest_WindowOperation{
			Event:   windowOp,
			Windows: windows,
		},
	}
	return d
}

// convertToUdfError converts the error returned by the reduceFn to ApplyUDFErr
func convertToUdfError(err error) ApplyUDFErr {
	// if any error happens in reduce
	// will exit and restart the numa container
	udfErr, _ := sdkerr.FromError(err)
	switch udfErr.ErrorKind() {
	case sdkerr.Retryable:
		// TODO: currently we don't handle retryable errors for reduce
		return ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	case sdkerr.NonRetryable:
		return ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	default:
		return ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	}
}

// parseReduceResponse parse the SDK response to TimedWindowResponse
func parseReduceResponse(response *reducepb.ReduceResponse, msgId string) *window.TimedWindowResponse {
	taggedMessage := &isb.WriteMessage{
		Message: isb.Message{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{
					EventTime: response.GetWindow().GetEnd().AsTime().Add(-1 * time.Millisecond),
					IsLate:    false,
				},
				Keys: response.GetResult().GetKeys(),
				ID:   msgId,
			},
			Body: isb.Body{
				Payload: response.GetResult().GetValue(),
			},
		},
		Tags: response.GetResult().GetTags(),
	}

	// we don't care about the combined key which was used for demultiplexing in sdk side
	// because for fixed and sliding we don't track keys.
	return &window.TimedWindowResponse{
		WriteMessage: taggedMessage,
		Window: window.NewAlignedTimedWindow(
			response.GetWindow().GetStart().AsTime(),
			response.GetWindow().GetEnd().AsTime(),
			response.GetWindow().GetSlot(),
		),
		// this will be set once the SDK has sent the last message
		EOF: response.GetEOF(),
	}
}
