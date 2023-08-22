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

package udsource

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

// GRPCBasedUDSource applies a user-defined source over gRPC
// connection where server is the UDSource.
type GRPCBasedUDSource struct {
	client sourceclient.Client
}

// NewUDSgRPCBasedUDSource accepts a gRPC client and returns a new GRPCBasedUDSource.
func NewUDSgRPCBasedUDSource(c sourceclient.Client) (*GRPCBasedUDSource, error) {
	return &GRPCBasedUDSource{c}, nil
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedUDSource) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the udsource is healthy.
func (u *GRPCBasedUDSource) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the udsource is connected.
func (u *GRPCBasedUDSource) WaitUntilReady(ctx context.Context) error {
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
func (u *GRPCBasedUDSource) ApplyPendingFn(ctx context.Context) (int64, error) {
	if resp, err := u.client.PendingFn(ctx, &emptypb.Empty{}); err == nil {
		return int64(resp.Result.Count), nil
	} else {
		return isb.PendingNotAvailable, err
	}
}

// ApplyReadFn reads messages from the source.
// TODO(udsource) - this should be able to simplify, also needs improvement.
func (u *GRPCBasedUDSource) ApplyReadFn(ctx context.Context, count int64, timeout time.Duration) ([]*isb.ReadMessage, error) {
	var readMessages []*isb.ReadMessage

	var r = &sourcepb.ReadRequest{
		Request: &sourcepb.ReadRequest_Request{
			NumRecords:  uint64(count),
			TimeoutInMs: uint32(timeout.Milliseconds()),
		},
	}
	// Call the client
	var datumCh = make(chan *sourcepb.ReadResponse)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := u.client.ReadFn(ctx, r, datumCh); err != nil {
			// TODO: handle error
			fmt.Printf("failed to read messages from udsource: %v\n", err)
		}
		defer close(datumCh)
	}()

	// Collect the messages from the channel and return
	for {
		println("I am in the loop")
		select {
		case <-ctx.Done():
			println("I am returning because context is done")
			return readMessages, fmt.Errorf("context is done, %w", ctx.Err())
		case datum, ok := <-datumCh:
			if ok {
				// Convert the datum to ReadMessage and append to the list
				r := datum.GetResult()
				println("Received message: ")
				println(r.Payload)
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
				println("I am returning because client.ReadFn is done")
				return readMessages, nil
			}
		}
	}
}

// ApplyAckFn acknowledges messages in the source.
func (u *GRPCBasedUDSource) ApplyAckFn(ctx context.Context, offsets []isb.Offset) error {
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
