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
func (u *GRPCBasedReduce) ApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.WriteMessage, error) {
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

				d := createDatum(msg)

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
			taggedMessages := make([]*isb.WriteMessage, 0)
			for _, response := range result.GetResults() {
				keys := response.Keys
				taggedMessage := &isb.WriteMessage{
					Message: isb.Message{
						Header: isb.Header{
							MessageInfo: isb.MessageInfo{
								EventTime: partitionID.End.Add(-1 * time.Millisecond),
								IsLate:    false,
							},
							Keys: keys,
						},
						Body: isb.Body{
							Payload: response.Value,
						},
					},
					Tags: response.Tags,
				}
				taggedMessages = append(taggedMessages, taggedMessage)
			}
			return taggedMessages, nil
		case <-ctx.Done():
			return nil, convertToUdfError(ctx.Err())
		}
	}
}

// AsyncApplyReduce accepts a channel of isbMessages and returns the aggregated result asynchronously on the responseCh channel
// and any error on the errCh channel, it doesn't wait for the output of all the keys to be available.
func (u *GRPCBasedReduce) AsyncApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) (<-chan []*isb.WriteMessage, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan []*isb.WriteMessage)
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
					// if the resultCh channel is closed, close the responseCh and errCh channels and return
					close(responseCh)
					return
				}
				responseCh <- convertToWriteMessages(result, partitionID)
			case err := <-reduceErrCh:
				if err != nil {
					errCh <- err
				}
			case <-ctx.Done():
				return
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

				d := createDatum(msg)

				// send the datum to datumCh channel, handle the case when the context is canceled
				datumCh <- d

			case <-ctx.Done(): // if the context is done, return
				return
			}
		}
	}()

	return responseCh, errCh
}

func createDatum(readMessage *isb.ReadMessage) *reducepb.ReduceRequest {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	parentMessageInfo := readMessage.MessageInfo
	var d = &reducepb.ReduceRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(readMessage.Watermark),
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

func convertToWriteMessages(response *reducepb.ReduceResponse, partitionID *partition.ID) []*isb.WriteMessage {
	taggedMessages := make([]*isb.WriteMessage, 0)
	for _, response := range response.GetResults() {
		keys := response.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: partitionID.End.Add(-1 * time.Millisecond),
						IsLate:    false,
					},
					Keys: keys,
				},
				Body: isb.Body{
					Payload: response.Value,
				},
			},
			Tags: response.Tags,
		}
		taggedMessages = append(taggedMessages, taggedMessage)
	}
	return taggedMessages
}
