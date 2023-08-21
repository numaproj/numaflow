package rpc

import (
	"context"
	"fmt"
	"time"

	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/isb"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapper"
	"github.com/numaproj/numaflow/pkg/udf"
)

type UDSgRPCBasedMap struct {
	client mapper.Client
}

//var _ mapapplier.MapApplier = (*UDSgRPCBasedMap)(nil)

func NewUDSgRPCBasedMap(client mapper.Client) *UDSgRPCBasedMap {
	return &UDSgRPCBasedMap{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedMap) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the map udf is healthy.
func (u *UDSgRPCBasedMap) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the map udf is connected.
func (u *UDSgRPCBasedMap) WaitUntilReady(ctx context.Context) error {
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

func (u *UDSgRPCBasedMap) ApplySourceTransform(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	parentMessageInfo := readMessage.MessageInfo
	var req = &mappb.MapRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(readMessage.Watermark),
	}

	response, err := u.client.MapFn(ctx, req)
	if err != nil {
		udfErr, _ := sdkerr.FromError(err)
		switch udfErr.ErrorKind() {
		case sdkerr.Retryable:
			var success bool
			_ = wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
				// retry every "duration * factor + [0, jitter]" interval for 5 times
				Duration: 1 * time.Second,
				Factor:   1,
				Jitter:   0.1,
				Steps:    5,
			}, func() (done bool, err error) {
				response, err = u.client.MapFn(ctx, req)
				if err != nil {
					udfErr, _ = sdkerr.FromError(err)
					switch udfErr.ErrorKind() {
					case sdkerr.Retryable:
						return false, nil
					case sdkerr.NonRetryable:
						return true, nil
					default:
						return true, nil
					}
				}
				success = true
				return true, nil
			})
			if !success {
				return nil, udf.ApplyUDFErr{
					UserUDFErr: false,
					Message:    fmt.Sprintf("gRPC client.SourceTransformFn failed, %s", err),
					InternalErr: udf.InternalErr{
						Flag:        true,
						MainCarDown: false,
					},
				}
			}
		case sdkerr.NonRetryable:
			return nil, udf.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.SourceTransformFn failed, %s", err),
				InternalErr: udf.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		default:
			return nil, udf.ApplyUDFErr{
				UserUDFErr: false,
				Message:    fmt.Sprintf("gRPC client.SourceTransformFn failed, %s", err),
				InternalErr: udf.InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
	}

	writeMessages := make([]*isb.WriteMessage, 0)
	for _, result := range response.GetResults() {
		keys := result.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: parentMessageInfo,
					Keys:        keys,
				},
				Body: isb.Body{
					Payload: result.Value,
				},
			},
			Tags: result.Tags,
		}
		writeMessages = append(writeMessages, taggedMessage)
	}
	return writeMessages, nil
}
