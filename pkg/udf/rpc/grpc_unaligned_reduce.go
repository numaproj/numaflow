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
	"strings"
	"time"

	sessionreducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/sessionreduce/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/sessionreducer"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/window"
)

// GRPCBasedUnalignedReduce is a reduce applier that uses gRPC client to invoke the session reduce UDF. It implements the applier.ReduceApplier interface.
type GRPCBasedUnalignedReduce struct {
	client     sessionreducer.Client
	resultsMap map[string]int
}

func NewGRPCBasedUnalignedReduce(client sessionreducer.Client) *GRPCBasedUnalignedReduce {
	return &GRPCBasedUnalignedReduce{
		client:     client,
		resultsMap: make(map[string]int),
	}
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedUnalignedReduce) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedUnalignedReduce) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the reduce udf is connected.
func (u *GRPCBasedUnalignedReduce) WaitUntilReady(ctx context.Context) error {
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

// ApplyReduce accepts a channel of timedWindowRequest and returns the result in a channel of timedWindowResponse.
// ApplyReduce will never return for unAligned (for-loops ever break) because we only have one single partition. Windows are handled outside.
func (u *GRPCBasedUnalignedReduce) ApplyReduce(ctx context.Context, partitionID *partition.ID, requestsStream <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *window.TimedWindowResponse)
		requestCh  = make(chan *sessionreducepb.SessionReduceRequest)
	)

	// pass key and window information inside the context
	mdMap := map[string]string{
		sdkclient.WinStartTime: strconv.FormatInt(partitionID.Start.UnixMilli(), 10),
		sdkclient.WinEndTime:   strconv.FormatInt(partitionID.End.UnixMilli(), 10),
	}

	grpcCtx := metadata.NewOutgoingContext(ctx, metadata.New(mdMap))

	// invoke the AsyncReduceFn method with requestCh channel and send the result to responseCh channel
	// and any error to errCh channel
	go func() {
		resultCh, reduceErrCh := u.client.SessionReduceFn(grpcCtx, requestCh)
		for {
			select {
			case result, ok := <-resultCh:
				if !ok || result == nil {
					// if the resultCh channel is closed, close the responseCh
					close(responseCh)
					return
				}
				responseCh <- u.parseSessionReduceResponse(result)
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

	// creates SessionWindowRequest from TimedWindowRequest and send it to requestCh channel for AsyncReduceFn
	go func() {
		// after reading all the messages from the requestsStream or if ctx was canceled close the requestCh channel
		defer func() {
			close(requestCh)
		}()
		for {
			select {
			case req, ok := <-requestsStream:
				// if the requestsStream is closed or if the message is nil, return
				if !ok || req == nil {
					return
				}

				d := createUnalignedReduceRequest(req)

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

// createUnalignedReduceRequest creates a unAlignedReduceRequest from TimedWindowRequest
func createUnalignedReduceRequest(windowRequest *window.TimedWindowRequest) *sessionreducepb.SessionReduceRequest {
	var windowOp sessionreducepb.SessionReduceRequest_WindowOperation_Event
	var partitions []*sessionreducepb.KeyedWindow

	for _, w := range windowRequest.Windows {
		partitions = append(partitions, &sessionreducepb.KeyedWindow{
			Start: timestamppb.New(w.StartTime()),
			End:   timestamppb.New(w.EndTime()),
			Slot:  w.Slot(),
			Keys:  w.Keys(),
		})
	}

	// we have to handle different scenarios unlike the Aligned window because
	// unAligned window width can dynamimcally change.
	switch windowRequest.Operation {
	case window.Open:
		windowOp = sessionreducepb.SessionReduceRequest_WindowOperation_OPEN
	case window.Close: // CLOSE is explicitly handled because we close at Key level, unlike Aligned where we close the whole window.
		windowOp = sessionreducepb.SessionReduceRequest_WindowOperation_CLOSE
	case window.Merge:
		windowOp = sessionreducepb.SessionReduceRequest_WindowOperation_MERGE
	case window.Expand:
		windowOp = sessionreducepb.SessionReduceRequest_WindowOperation_EXPAND
	default:
		windowOp = sessionreducepb.SessionReduceRequest_WindowOperation_APPEND
	}

	var payload = &sessionreducepb.SessionReduceRequest_Payload{}
	if windowRequest.ReadMessage != nil {
		payload = &sessionreducepb.SessionReduceRequest_Payload{
			Keys:      windowRequest.ReadMessage.Keys,
			Value:     windowRequest.ReadMessage.Payload,
			EventTime: timestamppb.New(windowRequest.ReadMessage.MessageInfo.EventTime),
			Watermark: timestamppb.New(windowRequest.ReadMessage.Watermark),
		}
	}

	var d = &sessionreducepb.SessionReduceRequest{
		Payload: payload,
		Operation: &sessionreducepb.SessionReduceRequest_WindowOperation{
			Event:        windowOp,
			KeyedWindows: partitions,
		},
	}
	return d
}

// parseSessionReduceResponse receives a SessionReduceResponse and parses it into a TimedWindowResponse
func (u *GRPCBasedUnalignedReduce) parseSessionReduceResponse(response *sessionreducepb.SessionReduceResponse) *window.TimedWindowResponse {

	rw := response.GetKeyedWindow()
	result := response.GetResult()

	start := rw.GetStart().AsTime()
	end := rw.GetEnd().AsTime()

	// generate the unique Id for the window to keep track of the response count for the window using the resultsMap
	uniqueId := fmt.Sprintf("%d:%d:%s:%s", start.UnixMilli(), end.UnixMilli(), rw.GetSlot(), strings.Join(rw.GetKeys(), ":"))

	// update the message count in resultsMap and get the message ID
	msgId := u.updateAndGetMsgId(uniqueId)

	// If EOF is received, delete the related entry in resultsMap and return a final response
	if response.GetEOF() {
		delete(u.resultsMap, uniqueId)
		return &window.TimedWindowResponse{
			Window: window.NewUnalignedTimedWindow(start, end, rw.GetSlot(), rw.GetKeys()),
			EOF:    true,
		}
	}

	// Construct the write message from the response
	taggedMessage := &isb.WriteMessage{
		Message: isb.Message{
			Header: isb.Header{
				ID: msgId,
				MessageInfo: isb.MessageInfo{
					EventTime: end.Add(-1 * time.Millisecond),
					IsLate:    false,
				},
				Keys: result.GetKeys(),
			},
			Body: isb.Body{
				Payload: result.GetValue(),
			},
		},
		Tags: result.GetTags(),
	}

	// Return the final TimedWindowResponse
	return &window.TimedWindowResponse{
		WriteMessage: taggedMessage,
		Window:       window.NewUnalignedTimedWindow(start, end, rw.GetSlot(), rw.GetKeys()),
		EOF:          response.GetEOF(),
	}
}

// updateMessageIDCount updates the message count in resultsMap and returns the updated message ID
func (u *GRPCBasedUnalignedReduce) updateAndGetMsgId(baseMsgId string) string {
	val, _ := u.resultsMap[baseMsgId]
	val++
	u.resultsMap[baseMsgId] = val
	return fmt.Sprintf("%s:%d", baseMsgId, val)
}
