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

// GRPCBasedReduce is a reduce applier that uses gRPC client to invoke the reduce UDF. It implements the applier.ReduceApplier interface.
type GRPCBasedReduce struct {
	client reducer.Client
}

func NewUDSgRPCBasedReduce(client reducer.Client) *GRPCBasedReduce {
	return &GRPCBasedReduce{client: client}
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedReduce) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedReduce) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the map udf is connected.
func (u *GRPCBasedReduce) WaitUntilReady(ctx context.Context) error {
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

// ApplyReduce accepts a channel of isbMessages and returns the aggregated result
func (u *GRPCBasedReduce) ApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *window.TimedWindowRequest) (*window.TimedWindowResponse, error) {
	var (
		result     *reducepb.ReduceResponse
		err        error
		errCh      = make(chan error, 1)
		responseCh = make(chan *reducepb.ReduceResponse, 1)
		datumCh    = make(chan *reducepb.ReduceRequest)
	)

	// pass key and window information inside the context
	mdMap := map[string]string{
		sdkclient.WinStartTime: strconv.FormatInt(partitionID.Start.UnixMilli(), 10),
		sdkclient.WinEndTime:   strconv.FormatInt(partitionID.End.UnixMilli(), 10),
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	grpcCtx := metadata.NewOutgoingContext(ctx, metadata.New(mdMap))

	// There can be two error scenarios:
	// 1. The u.client.ReduceFn method returns an error before reading all the messages from the messageStream
	// 2. The u.client.ReduceFn method returns an error after reading all the messages from the messageStream

	// invoke the reduceFn method with datumCh channel
	go func() {
		result, err = u.client.ReduceFn(grpcCtx, datumCh)
		if err != nil {
			errCh <- err
		} else {
			responseCh <- result
		}
		close(errCh)
		close(responseCh)
	}()

	// create datum from isbMessage and send it to datumCh channel for reduceFn
	go func() {
		// after reading all the messages from the messageStream or if ctx was canceled close the datumCh channel
		defer close(datumCh)
		for {
			select {
			case msg, ok := <-messageStream:
				// if the messageStream is closed or if the message is nil, return
				if !ok || msg == nil {
					return
				}

				d := createReduceRequest(msg)

				// send the datum to datumCh channel, handle the case when the context is canceled
				select {
				case datumCh <- d:
				case <-ctx.Done():
					return
				}

			case <-ctx.Done(): // if the context is done, return
				return
			}
		}
	}()

	// wait for the reduceFn to finish
	for {
		select {
		case err = <-errCh:
			if err != nil {
				return nil, convertToUdfError(err)
			}
		case result = <-responseCh:
			return parseReduceResponse(result), nil
		case <-ctx.Done():
			return nil, convertToUdfError(ctx.Err())
		}
	}
}

// AsyncApplyReduce accepts a channel of timedWindowRequest and returns the result in a channel of timedWindowResponse
func (u *GRPCBasedReduce) AsyncApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *window.TimedWindowResponse)
		datumCh    = make(chan *reducepb.ReduceRequest)
	)

	// pass key and window information inside the context
	mdMap := map[string]string{
		sdkclient.WinStartTime: strconv.FormatInt(partitionID.Start.UnixMilli(), 10),
		sdkclient.WinEndTime:   strconv.FormatInt(partitionID.End.UnixMilli(), 10),
	}

	grpcCtx := metadata.NewOutgoingContext(ctx, metadata.New(mdMap))

	// invoke the AsyncReduceFn method with datumCh channel and send the result to responseCh channel
	// and any error to errCh channel
	go func() {
		resultCh, reduceErrCh := u.client.AsyncReduceFn(grpcCtx, datumCh)
		for {
			select {
			case result, ok := <-resultCh:
				if !ok || result == nil {
					// if the resultCh channel is closed, close the responseCh and return
					close(responseCh)
					return
				}
				responseCh <- parseReduceResponse(result)
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

	// create datum from isbMessage and send it to datumCh channel for AsyncReduceFn
	go func() {
		// after reading all the messages from the messageStream or if ctx was canceled close the datumCh channel
		defer func() {
			close(datumCh)
		}()
		for {
			select {
			case msg, ok := <-messageStream:
				// if the messageStream is closed or if the message is nil, return
				if !ok || msg == nil {
					return
				}

				d := createReduceRequest(msg)
				// send the datum to datumCh channel, handle the case when the context is canceled
				select {
				case datumCh <- d:
				case <-ctx.Done():
					return
				}

			case <-ctx.Done(): // if the context is done, don't send any more datum to datumCh channel
				return
			}
		}
	}()

	return responseCh, errCh
}

func createReduceRequest(windowRequest *window.TimedWindowRequest) *reducepb.ReduceRequest {
	var windowOp reducepb.ReduceRequest_WindowOperation_Event
	var partitions []*reducepb.Partition

	for _, w := range windowRequest.Windows {
		partitions = append(partitions, &reducepb.Partition{
			Start: timestamppb.New(w.StartTime()),
			End:   timestamppb.New(w.EndTime()),
			Slot:  w.Slot(),
		})
	}
	// for fixed and sliding window event can be either open or append
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
			Event:      windowOp,
			Partitions: partitions,
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

func parseReduceResponse(response *reducepb.ReduceResponse) *window.TimedWindowResponse {
	taggedMessages := make([]*isb.WriteMessage, 0)
	for _, result := range response.GetResults() {
		keys := result.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: response.GetEventTime().AsTime(),
						IsLate:    false,
					},
					Keys: keys,
				},
				Body: isb.Body{
					Payload: result.Value,
				},
			},
			Tags: result.Tags,
		}
		taggedMessages = append(taggedMessages, taggedMessage)
	}

	// we don't care about the combined key which was used for demultiplexing in sdk side
	// because for fixed and sliding we don't track keys.
	return &window.TimedWindowResponse{
		WriteMessages: taggedMessages,
		ID: &partition.ID{
			Start: response.GetPartition().GetStart().AsTime(),
			End:   response.GetPartition().GetEnd().AsTime(),
			Slot:  response.GetPartition().GetSlot(),
		},
		CombinedKey: "",
	}
}
