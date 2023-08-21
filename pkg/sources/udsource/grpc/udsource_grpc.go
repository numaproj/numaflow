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

/*
Package grpc defines
*/
package grpc

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"

	"github.com/numaproj/numaflow/pkg/isb"
	sourceclient "github.com/numaproj/numaflow/pkg/sdkclient/source/client"
	"github.com/numaproj/numaflow/pkg/sources/udsource/utils"

	"google.golang.org/protobuf/types/known/emptypb"
)

// UDSgRPCBasedUDSource applies a user-defined source over gRPC
// (over Unix Domain Socket) client/server where server is the UDSource.
type UDSgRPCBasedUDSource struct {
	client sourceclient.Client
}

// NewUDSgRPCBasedUDSource returns UDSgRPCBasedUDSource
func NewUDSgRPCBasedUDSource() (*UDSgRPCBasedUDSource, error) {
	c, err := sourceclient.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &UDSgRPCBasedUDSource{c}, nil
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedUDSource) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the udsource is healthy.
func (u *UDSgRPCBasedUDSource) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the udsource is connected.
func (u *UDSgRPCBasedUDSource) WaitUntilReady(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// ApplyPendingFn returns the number of pending messages in the source.
func (u *UDSgRPCBasedUDSource) ApplyPendingFn(ctx context.Context) (int64, error) {
	if resp, err := u.client.PendingFn(ctx, &emptypb.Empty{}); err == nil {
		return int64(resp.Result.Count), nil
	} else {
		return isb.PendingNotAvailable, err
	}
}

// ApplyReadFn reads messages from the source.
// TODO(udsource) Should we pass in a channel and stream the messages?
func (u *UDSgRPCBasedUDSource) ApplyReadFn(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
	var readMessages []*isb.ReadMessage

	// Construct the request
	// TODO(udsource) - add timeout to the request
	var r = &sourcepb.ReadRequest{
		Request: &sourcepb.ReadRequest_Request{
			NumRecords: uint64(count),
		},
	}
	// Call the client
	var datumCh = make(chan *sourcepb.ReadResponse)
	defer close(datumCh)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := u.client.ReadFn(ctx, r, datumCh); err != nil {
			// TODO: handle error
			fmt.Printf("failed to read messages from udsource: %v\n", err)
		}
	}()

	// Collect the messages from the channel and return
	for {
		select {
		case <-ctx.Done():
			return readMessages, fmt.Errorf("context is done, %w", ctx.Err())
		case datum, ok := <-datumCh:
			if ok {
				// Convert the datum to ReadMessage and append to the list
				r := datum.GetResult()
				readMessage := &isb.ReadMessage{
					Message: isb.Message{
						Header: isb.Header{
							MessageInfo: isb.MessageInfo{EventTime: r.GetEventTime().AsTime()},
							// TODO(udsource) - construct id from the source partition ID and offset
							ID: "id",
						},
						Body: isb.Body{
							Payload: r.GetPayload(),
						},
					},
					ReadOffset: ConvertToIsbOffset(r.GetOffset()),
				}
				readMessages = append(readMessages, readMessage)
			} else {
				wg.Wait()
				return readMessages, nil
			}
		}
	}
}

// ApplyAckFn acknowledges messages in the source.
func (u *UDSgRPCBasedUDSource) ApplyAckFn(ctx context.Context, offsets []isb.Offset) error {
	rOffsets := make([]*sourcepb.Offset, len(offsets))
	for i, offset := range offsets {
		rOffsets[i] = ConvertToSourceOffset(offset)
	}
	var r = &sourcepb.AckRequest{
		Request: &sourcepb.AckRequest_Request{
			Offsets: rOffsets,
		},
	}
	_, err := u.client.AckFn(ctx, r)
	return err
}

func ConvertToSourceOffset(offset isb.Offset) *sourcepb.Offset {
	return &sourcepb.Offset{
		PartitionId: strconv.Itoa(int(offset.PartitionIdx())),
		Offset:      []byte(offset.String()),
	}
}

func ConvertToIsbOffset(offset *sourcepb.Offset) isb.Offset {
	if partitionIdx, err := strconv.Atoi(offset.GetPartitionId()); err != nil {
		return utils.NewSimpleSourceOffset(string(offset.Offset), utils.DefaultPartitionIdx)
	} else {
		return utils.NewSimpleSourceOffset(string(offset.Offset), int32(partitionIdx))
	}
}
