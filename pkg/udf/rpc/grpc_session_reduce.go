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

// GRPCBasedSessionReduce is a reduce applier that uses gRPC client to invoke the reduce UDF. It implements the applier.ReduceApplier interface.
type GRPCBasedSessionReduce struct {
	client sessionreducer.Client
}

func NewGRPCBasedSessionReduce(client sessionreducer.Client) *GRPCBasedSessionReduce {
	return &GRPCBasedSessionReduce{client: client}
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedSessionReduce) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedSessionReduce) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the map udf is connected.
func (u *GRPCBasedSessionReduce) WaitUntilReady(ctx context.Context) error {
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
func (u *GRPCBasedSessionReduce) ApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *window.TimedWindowRequest) ([]*isb.WriteMessage, error) {
	return nil, fmt.Errorf("not implemented")
}

// AsyncApplyReduce accepts a channel of timedWindowRequest and returns the result in a channel of timedWindowResponse
func (u *GRPCBasedSessionReduce) AsyncApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *window.TimedWindowRequest) (<-chan *window.TimedWindowResponse, <-chan error) {
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
					// if the resultCh channel is closed, close the responseCh and errCh channels and return
					close(responseCh)
					return
				}
				responseCh <- parseSessionReduceResponse(result)
			case err := <-reduceErrCh:
				if err != nil {
					errCh <- err
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// create datum from isbMessage and send it to requestCh channel for AsyncReduceFn
	go func() {
		// after reading all the messages from the messageStream or if ctx was canceled close the requestCh channel
		defer func() {
			close(requestCh)
		}()
		for {
			select {
			case msg, ok := <-messageStream:
				// if the messageStream is closed or if the message is nil, return
				if !ok || msg == nil {
					return
				}

				d := createSessionReduceRequest(msg)

				// send the datum to requestCh channel, handle the case when the context is canceled
				requestCh <- d

			case <-ctx.Done(): // if the context is done, return
				return
			}
		}
	}()

	return responseCh, errCh
}

func createSessionReduceRequest(windowRequest *window.TimedWindowRequest) *sessionreducepb.SessionReduceRequest {
	var windowOp sessionreducepb.SessionReduceRequest_WindowOperation_Event
	var partitions []*sessionreducepb.Partition

	for _, w := range windowRequest.Windows {
		partitions = append(partitions, &sessionreducepb.Partition{
			Start: timestamppb.New(w.StartTime()),
			End:   timestamppb.New(w.EndTime()),
			Slot:  w.Slot(),
		})
	}
	// for fixed and sliding window event can be either open, close or append
	switch windowRequest.Event {
	case window.Open:
		windowOp = sessionreducepb.SessionReduceRequest_WindowOperation_OPEN
	case window.Close:
		windowOp = sessionreducepb.SessionReduceRequest_WindowOperation_CLOSE
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
			Event:      windowOp,
			Partitions: partitions,
		},
	}
	return d
}

func parseSessionReduceResponse(response *sessionreducepb.SessionReduceResponse) *window.TimedWindowResponse {
	taggedMessages := make([]*isb.WriteMessage, 0)
	for _, result := range response.GetResults() {
		keys := result.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: response.EventTime.AsTime(),
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

	return &window.TimedWindowResponse{
		WriteMessages: taggedMessages,
		ID: &partition.ID{
			Start: response.GetPartition().GetStart().AsTime(),
			End:   response.GetPartition().GetEnd().AsTime(),
			Slot:  response.GetPartition().GetSlot(),
		},
		CombinedKey: response.CombinedKey,
	}
}
