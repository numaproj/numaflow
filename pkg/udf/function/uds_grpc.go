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

package function

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	map_applier "github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	reduce_applier "github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

// UDSgRPCBasedUDF applies user defined function over gRPC (over Unix Domain Socket) client/server where server is the UDF.
type UDSgRPCBasedUDF struct {
	client functionsdk.Client
}

var _ map_applier.MapApplier = (*UDSgRPCBasedUDF)(nil)
var _ reduce_applier.ReduceApplier = (*UDSgRPCBasedUDF)(nil)

// NewUDSgRPCBasedUDF returns a new UDSgRPCBasedUDF object.
func NewUDSgRPCBasedUDF(c functionsdk.Client) (*UDSgRPCBasedUDF, error) {
	return &UDSgRPCBasedUDF{c}, nil
}

// NewUDSgRPCBasedUDFWithClient need this for testing
func NewUDSgRPCBasedUDFWithClient(client functionsdk.Client) *UDSgRPCBasedUDF {
	return &UDSgRPCBasedUDF{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedUDF) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the udf is healthy.
func (u *UDSgRPCBasedUDF) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the udf is connected.
func (u *UDSgRPCBasedUDF) WaitUntilReady(ctx context.Context) error {
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

func (u *UDSgRPCBasedUDF) ApplyMap(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
	key := readMessage.Key
	payload := readMessage.Body.Payload
	offset := readMessage.ReadOffset
	parentPaneInfo := readMessage.PaneInfo
	var d = &functionpb.Datum{
		Key:       key,
		Value:     payload,
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(parentPaneInfo.EventTime)},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(readMessage.Watermark)},
	}

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{functionsdk.DatumKey: key}))
	datumList, err := u.client.MapFn(ctx, d)
	if err != nil {
		return nil, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	}

	writeMessages := make([]*isb.Message, 0)
	for i, datum := range datumList {
		key := datum.Key
		writeMessage := &isb.Message{
			Header: isb.Header{
				PaneInfo: parentPaneInfo,
				ID:       fmt.Sprintf("%s-%d", offset.String(), i),
				Key:      key,
			},
			Body: isb.Body{
				Payload: datum.Value,
			},
		}
		writeMessages = append(writeMessages, writeMessage)
	}
	return writeMessages, nil
}

// should we pass metadata information ?

// ApplyReduce accepts a channel of isbMessages and returns the aggregated result
func (u *UDSgRPCBasedUDF) ApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	datumCh := make(chan *functionpb.Datum)
	var wg sync.WaitGroup
	var result []*functionpb.Datum
	var err error

	// pass key and window information inside the context
	mdMap := map[string]string{
		functionsdk.DatumKey:     partitionID.Slot,
		functionsdk.WinStartTime: strconv.FormatInt(partitionID.Start.UnixMilli(), 10),
		functionsdk.WinEndTime:   strconv.FormatInt(partitionID.End.UnixMilli(), 10),
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
					return nil, ctx.Err()
				}
			}
			if !ok {
				break readLoop
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// close the datumCh, let the reduceFn know that there are no more messages
	close(datumCh)

	wg.Wait()

	if err != nil {
		return nil, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	}

	writeMessages := make([]*isb.Message, 0)
	for _, datum := range result {
		key := datum.Key
		writeMessage := &isb.Message{
			Header: isb.Header{
				PaneInfo: isb.PaneInfo{
					EventTime: partitionID.End,
					StartTime: partitionID.Start,
					EndTime:   partitionID.End,
					IsLate:    false,
				},
				Key: key,
			},
			Body: isb.Body{
				Payload: datum.Value,
			},
		}
		writeMessages = append(writeMessages, writeMessage)
	}
	return writeMessages, nil
}

func createDatum(readMessage *isb.ReadMessage) *functionpb.Datum {
	key := readMessage.Key
	payload := readMessage.Body.Payload
	parentPaneInfo := readMessage.PaneInfo

	var d = &functionpb.Datum{
		Key:       key,
		Value:     payload,
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(parentPaneInfo.EventTime)},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(readMessage.Watermark)},
	}
	return d
}
