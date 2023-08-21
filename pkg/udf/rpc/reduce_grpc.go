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
	reducer "github.com/numaproj/numaflow/pkg/sdkclient/reducer"
	"github.com/numaproj/numaflow/pkg/udf"
)

type UDSgRPCBasedReduce struct {
	client reducer.Client
}

func NewUDSgRPCBasedReduce(client reducer.Client) *UDSgRPCBasedReduce {
	return &UDSgRPCBasedReduce{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedReduce) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the map udf is healthy.
func (u *UDSgRPCBasedReduce) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the map udf is connected.
func (u *UDSgRPCBasedReduce) WaitUntilReady(ctx context.Context) error {
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

// ApplyReduce accepts a channel of isbMessages and returns the aggregated result
func (u *UDSgRPCBasedReduce) ApplyReduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.WriteMessage, error) {
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
			return nil, udf.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
				InternalErr: udf.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		case sdkerr.NonRetryable:
			return nil, udf.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
				InternalErr: udf.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		default:
			return nil, udf.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
				InternalErr: udf.InternalErr{
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
