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
	clientsdk "github.com/numaproj/numaflow/pkg/sdkclient/udf/client"
	"strconv"
	"sync"
	"time"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/udferr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"

	map_applier "github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	reduce_applier "github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

// UDSgRPCBasedUDF applies user defined function over gRPC (over Unix Domain Socket) client/server where server is the UDF.
type UDSgRPCBasedUDF struct {
	client clientsdk.Client
}

var _ map_applier.MapApplier = (*UDSgRPCBasedUDF)(nil)
var _ reduce_applier.ReduceApplier = (*UDSgRPCBasedUDF)(nil)

// NewUDSgRPCBasedUDF returns a new UDSgRPCBasedUDF object.
func NewUDSgRPCBasedUDF(c clientsdk.Client) (*UDSgRPCBasedUDF, error) {
	return &UDSgRPCBasedUDF{c}, nil
}

// NewUDSgRPCBasedUDFWithClient need this for testing
func NewUDSgRPCBasedUDFWithClient(client clientsdk.Client) *UDSgRPCBasedUDF {
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

func (u *UDSgRPCBasedUDF) ApplyMap(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	offset := readMessage.ReadOffset
	parentMessageInfo := readMessage.MessageInfo
	id := readMessage.Message.ID
	numDelivered := readMessage.Metadata.NumDelivered
	var d = &functionpb.DatumRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(parentMessageInfo.EventTime)},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(readMessage.Watermark)},
		Metadata: &functionpb.Metadata{
			Id:           id,
			NumDelivered: numDelivered,
		},
	}

	datumList, err := u.client.MapFn(ctx, d)
	if err != nil {
		udfErr, _ := udferr.FromError(err)
		switch udfErr.ErrorKind() {
		case udferr.Retryable:
			var success bool
			_ = wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
				// retry every "duration * factor + [0, jitter]" interval for 5 times
				Duration: 1 * time.Second,
				Factor:   1,
				Jitter:   0.1,
				Steps:    5,
			}, func() (done bool, err error) {
				datumList, err = u.client.MapFn(ctx, d)
				if err != nil {
					udfErr, _ = udferr.FromError(err)
					switch udfErr.ErrorKind() {
					case udferr.Retryable:
						return false, nil
					case udferr.NonRetryable:
						return true, nil
					default:
						return true, nil
					}
				}
				success = true
				return true, nil
			})
			if !success {
				return nil, ApplyUDFErr{
					UserUDFErr: false,
					Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
					InternalErr: InternalErr{
						Flag:        true,
						MainCarDown: false,
					},
				}
			}
		case udferr.NonRetryable:
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		default:
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
	}

	writeMessages := make([]*isb.WriteMessage, 0)
	for i, datum := range datumList {
		keys := datum.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					ID:          fmt.Sprintf("%s-%d", offset.String(), i),
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: datum.Value,
				},
			},
			Tags: datum.Tags,
		}
		writeMessages = append(writeMessages, taggedMessage)
	}
	return writeMessages, nil
}

func (u *UDSgRPCBasedUDF) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	defer close(writeMessageCh)

	keys := message.Keys
	payload := message.Body.Payload
	offset := message.ReadOffset
	parentMessageInfo := message.MessageInfo
	id := message.Message.ID
	numDelivered := message.Metadata.NumDelivered
	var d = &functionpb.DatumRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(parentMessageInfo.EventTime)},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(message.Watermark)},
		Metadata: &functionpb.Metadata{
			Id:           id,
			NumDelivered: numDelivered,
		},
	}

	datumCh := make(chan *functionpb.DatumResponse)
	errs, ctx := errgroup.WithContext(ctx)
	errs.Go(func() error {
		err := u.client.MapStreamFn(ctx, d, datumCh)
		if err != nil {
			err = ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.MapStreamFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
			return err
		}
		return nil
	})

	i := 0
	for datum := range datumCh {
		i++
		keys := datum.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					ID:          fmt.Sprintf("%s-%d", offset.String(), i),
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: datum.Value,
				},
			},
			Tags: datum.Tags,
		}
		writeMessageCh <- *taggedMessage
	}

	return errs.Wait()
}

// should we pass metadata information ?

// ApplyReduce accepts a channel of isbMessages and returns the aggregated result
func (u *UDSgRPCBasedUDF) ApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	datumCh := make(chan *functionpb.DatumRequest)
	var wg sync.WaitGroup
	var result []*functionpb.DatumResponse
	var err error

	// pass key and window information inside the context
	mdMap := map[string]string{
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
		// if any error happens in reduce
		// will exit and restart the numa container
		udfErr, _ := udferr.FromError(err)
		switch udfErr.ErrorKind() {
		case udferr.Retryable:
			// TODO: currently we don't handle retryable errors for reduce
			return nil, ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		case udferr.NonRetryable:
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
	for _, datum := range result {
		keys := datum.Keys
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
					Payload: datum.Value,
				},
			},
			Tags: datum.Tags,
		}
		taggedMessages = append(taggedMessages, taggedMessage)
	}
	return taggedMessages, nil
}

func createDatum(readMessage *isb.ReadMessage) *functionpb.DatumRequest {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	parentMessageInfo := readMessage.MessageInfo
	id := readMessage.Message.ID
	numDelivered := readMessage.Metadata.NumDelivered
	var d = &functionpb.DatumRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(parentMessageInfo.EventTime)},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(readMessage.Watermark)},
		Metadata: &functionpb.Metadata{
			Id:           id,
			NumDelivered: numDelivered,
		},
	}
	return d
}
