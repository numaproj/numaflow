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
	"sync"
	"time"

	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"github.com/numaproj/numaflow-go/pkg/shared"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
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
	datumCh := make(chan *reducepb.ReduceRequest)
	var wg sync.WaitGroup
	var result *reducepb.ReduceResponse
	var err error

	// pass key and window information inside the context
	mdMap := map[string]string{
		shared.WinStartTime: strconv.FormatInt(partitionID.Start.UnixMilli(), 10),
		shared.WinEndTime:   strconv.FormatInt(partitionID.End.UnixMilli(), 10),
	}

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(mdMap))

	// invoke the reduceFn method with datumCh channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		// TODO handle this error here itself
		result, err = u.client.ReduceFn(ctx, datumCh)
	}()

readLoop:
	for {
		select {
		case msg, ok := <-messageStream:
			if msg != nil {
				d := createDatum(msg)
				select {
				case datumCh <- d:
				case <-ctx.Done():
					close(datumCh)
					return nil, ctx.Err()
				}
			}
			if !ok {
				break readLoop
			}
		case <-ctx.Done():
			close(datumCh)
			return nil, ctx.Err()
		}
	}

	// close the datumCh, let the reduceFn know that there are no more messages
	close(datumCh)

	wg.Wait()

	if err != nil {
		// if any error happens in reduce
		// will exit and restart the numa container
		udfErr, _ := sdkerr.FromError(err)
		switch udfErr.ErrorKind() {
		case sdkerr.Retryable:
			// TODO: currently we don't handle retryable errors for reduce
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		case sdkerr.NonRetryable:
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		default:
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
	}

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
