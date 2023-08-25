package sideinput

import (
	"context"
	"fmt"
	"time"

	sideinputpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1"
	"github.com/numaproj/numaflow-go/pkg/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt sideinputpb.SideInputClient
}

var _ Client = (*client)(nil)

// New creates a new client object.
func New(inputOptions ...Option) (*client, error) {
	var opts = &options{
		sockAddr:       shared.SideInputAddr,
		maxMessageSize: 1024 * 1024 * 64, // 64 MB
	}
	for _, inputOption := range inputOptions {
		inputOption(opts)
	}
	_, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c := new(client)
	sockAddr := fmt.Sprintf("%s:%s", shared.UDS, opts.sockAddr)
	conn, err := grpc.Dial(sockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.maxMessageSize), grpc.MaxCallSendMsgSize(opts.maxMessageSize)))
	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}
	c.conn = conn
	c.grpcClt = sideinputpb.NewSideInputClient(conn)
	return c, nil
}

// NewFromClient creates a new client object from a grpc client. This is used for testing.
func NewFromClient(c sideinputpb.SideInputClient) (Client, error) {
	return &client{
		grpcClt: c,
	}, nil
}

// CloseConn closes the grpc connection.
func (c client) CloseConn(ctx context.Context) error {
	return c.conn.Close()
}

// IsReady checks if the grpc connection is ready to use.
func (c client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

// RetrieveSideInput retrieves the side input value and returns the updated payload.
func (c client) RetrieveSideInput(ctx context.Context, in *emptypb.Empty) (*sideinputpb.SideInputResponse, error) {
	retrieveResponse, err := c.grpcClt.RetrieveSideInput(ctx, in)
	// TODO check which error to use
	if err != nil {
		return nil, fmt.Errorf("failed to execute c.grpcClt.RetrieveSideInput(): %w", err)
	}
	return retrieveResponse, nil
}

// IsHealthy checks if the client is healthy.
func (c client) IsHealthy(ctx context.Context) error {
	return c.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the client is connected.
func (c client) WaitUntilReady(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := c.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}
