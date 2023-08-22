package rpc

import (
	"context"
	"fmt"
	"time"

	mapstreampb "github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	mapstreamer "github.com/numaproj/numaflow/pkg/sdkclient/mapstreamer"
)

type UDSgRPCBasedMapStream struct {
	client mapstreamer.Client
}

//var _ mapapplier.MapApplier = (*UDSgRPCBasedMapStream)(nil)

func NewUDSgRPCBasedMapStream(client mapstreamer.Client) *UDSgRPCBasedMapStream {
	return &UDSgRPCBasedMapStream{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedMapStream) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the map stream udf is healthy.
func (u *UDSgRPCBasedMapStream) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the map stream udf is connected.
func (u *UDSgRPCBasedMapStream) WaitUntilReady(ctx context.Context) error {
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

func (u *UDSgRPCBasedMapStream) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	defer close(writeMessageCh)

	keys := message.Keys
	payload := message.Body.Payload
	offset := message.ReadOffset
	parentMessageInfo := message.MessageInfo

	var d = &mapstreampb.MapStreamRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(message.Watermark),
	}

	responseCh := make(chan *mapstreampb.MapStreamResponse)
	errs, ctx := errgroup.WithContext(ctx)
	errs.Go(func() error {
		err := u.client.MapStreamFn(ctx, d, responseCh)
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
	for response := range responseCh {
		result := response.Result
		i++
		keys := result.GetKeys()
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					ID:          fmt.Sprintf("%s-%d", offset.String(), i),
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: result.GetValue(),
				},
			},
			Tags: result.GetTags(),
		}
		writeMessageCh <- *taggedMessage
	}

	return errs.Wait()
}
