package clienttest

import (
	"context"
	"fmt"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1/sinkmock"
	sinksdk "github.com/numaproj/numaflow/pkg/sink-client/client"
	"google.golang.org/protobuf/types/known/emptypb"
)

// client contains the grpc client for testing.
type client struct {
	grpcClt sinkpb.UserDefinedSinkClient
}

var _ sinksdk.Client = (*client)(nil)

// New creates a new mock client object.
func New(c *sinkmock.MockUserDefinedSinkClient) (*client, error) {
	return &client{c}, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(ctx context.Context) error {
	return nil
}

// IsReady returns true if the grpc connection is ready to use.
func (c *client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

// SinkFn applies a function to a list of datum elements.
func (c *client) SinkFn(ctx context.Context, datumList []*sinkpb.DatumRequest) ([]*sinkpb.Response, error) {
	stream, err := c.grpcClt.SinkFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute c.grpcClt.SinkFn(): %w", err)
	}
	for _, datum := range datumList {
		if err := stream.Send(datum); err != nil {
			return nil, fmt.Errorf("failed to execute stream.Send(%v): %w", datum, err)
		}
	}
	responseList, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("failed to execute stream.CloseAndRecv(): %w", err)
	}

	return responseList.GetResponses(), nil
}
