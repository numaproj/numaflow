package applier

import (
	"context"
	"fmt"
	"time"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow-go/pkg/function/client"
	"github.com/numaproj/numaflow/pkg/isb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UDSGRPCBasedUDF applies user defined function over gRPC (over Unix Domain Socket) client/server where server is the UDF.
type UDSGRPCBasedUDF struct {
	client functionsdk.Client
}

var _ Applier = (*UDSGRPCBasedUDF)(nil)

// NewUDSGRPCBasedUDF returns a new UDSGRPCBasedUDF object.
func NewUDSGRPCBasedUDF() (*UDSGRPCBasedUDF, error) {
	c, err := client.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &UDSGRPCBasedUDF{c}, nil
}

// CloseConn closes the gRPC client connection.
func (u *UDSGRPCBasedUDF) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the client is connected.
func (u *UDSGRPCBasedUDF) WaitUntilReady(ctx context.Context) error {
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

func (u *UDSGRPCBasedUDF) Apply(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
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
