package function

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/udf/reducer"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow-go/pkg/function/client"
	"github.com/numaproj/numaflow/pkg/isb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// udsGRPCBasedUDF applies user defined function over gRPC (over Unix Domain Socket) client/server where server is the UDF.
type udsGRPCBasedUDF struct {
	client functionsdk.Client
}

var _ applier.Applier = (*udsGRPCBasedUDF)(nil)
var _ reducer.Reducer = (*udsGRPCBasedUDF)(nil)

// NewUDSGRPCBasedUDF returns a new udsGRPCBasedUDF object.
func NewUDSGRPCBasedUDF() (*udsGRPCBasedUDF, error) {
	c, err := client.New() // Can we pass this as a parameter to the function?
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &udsGRPCBasedUDF{c}, nil
}

// NewUdsGRPCBasedUDFWithClient need this for testing
func NewUdsGRPCBasedUDFWithClient(client functionsdk.Client) *udsGRPCBasedUDF {
	return &udsGRPCBasedUDF{client: client}
}

// CloseConn closes the gRPC client connection.
func (u *udsGRPCBasedUDF) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the client is connected.
func (u *udsGRPCBasedUDF) WaitUntilReady(ctx context.Context) error {
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

func (u *udsGRPCBasedUDF) Apply(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
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

// Reduce accepts a channel of isbMessages and returns the aggregated result
func (u *udsGRPCBasedUDF) Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	datumCh := make(chan *functionpb.Datum)
	var wg sync.WaitGroup
	var result []*functionpb.Datum
	var err error

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{functionsdk.DatumKey: partitionID.Key}))

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
