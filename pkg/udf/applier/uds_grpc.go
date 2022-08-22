package applier

import (
	"context"
	"fmt"
	"time"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/function/client"
	"github.com/numaproj/numaflow/pkg/isb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TODO: only support map operation ATM

type UDSGRPCBasedUDF struct {
	client *client.Client
}

var _ Applier = (*UDSGRPCBasedUDF)(nil)

func NewUDSGRPCBasedUDF(ctx context.Context) (*UDSGRPCBasedUDF, error) {
	c, err := client.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}
	return &UDSGRPCBasedUDF{c}, nil
}

func (u *UDSGRPCBasedUDF) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

func (u *UDSGRPCBasedUDF) WaitUntilReady(ctx context.Context) error {
	var err error
	for {
		select {
		case <-ctx.Done():
			// TODO: can use only one %w
			return fmt.Errorf("failed to wait for ready: %v, %w", ctx.Err(), err)
		default:
			if _, err = u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (u *UDSGRPCBasedUDF) Apply(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
	key := string(readMessage.Key)
	payload := readMessage.Body.Payload
	offset := readMessage.ReadOffset
	parentPaneInfo := readMessage.PaneInfo

	// TODO: revisit EventTime, IntervalWindow, and PaneInfo
	var d = &functionpb.Datum{
		Key:            key,
		Value:          payload,
		EventTime:      &functionpb.EventTime{EventTime: timestamppb.New(parentPaneInfo.EventTime)},
		IntervalWindow: &functionpb.IntervalWindow{StartTime: timestamppb.New(parentPaneInfo.StartTime), EndTime: timestamppb.New(parentPaneInfo.EndTime)},
		PaneInfo:       &functionpb.PaneInfo{Watermark: timestamppb.New(time.Time{})}, // TODO: insert the correct watermark
	}

	datumList, err := u.client.DoFn(ctx, d)
	if err != nil {
		return nil, ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("grpc client.DoFn failed, %s", err),
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
				Key:      []byte(key),
			},
			Body: isb.Body{
				Payload: datum.Value,
			},
		}
		writeMessages = append(writeMessages, writeMessage)
	}
	return writeMessages, nil
}
