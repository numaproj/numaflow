package udsideinput

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1"
	"google.golang.org/protobuf/types/known/emptypb"

	sideinputclient "github.com/numaproj/numaflow/pkg/sdkclient/sideinput/client"
)

// UDSgRPCBasedUDSideinput applies user defined side input over gRPC (over Unix Domain Socket) client/server
// where server is the UDSideInput.
type UDSgRPCBasedUDSideinput struct {
	client sideinputclient.Client
}

// NewUDSgRPCBasedUDSideinput returns UDSgRPCBasedUDSideinput
func NewUDSgRPCBasedUDSideinput() (*UDSgRPCBasedUDSideinput, error) {
	c, err := sideinputclient.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create a new gRPC client: %w", err)
	}
	return &UDSgRPCBasedUDSideinput{c}, nil
}

// CloseConn closes the gRPC client connection.
func (u *UDSgRPCBasedUDSideinput) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// IsHealthy checks if the udsideinput is healthy.
func (u *UDSgRPCBasedUDSideinput) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// WaitUntilReady waits until the udsideinput is connected.
func (u *UDSgRPCBasedUDSideinput) WaitUntilReady(ctx context.Context) error {
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

// Apply applies the retrieve side input function and returns the requested payload.
func (u *UDSgRPCBasedUDSideinput) Apply(ctx context.Context) (*v1.SideInputResponse, error) {
	resp, err := u.client.RetrieveSideInput(ctx, &emptypb.Empty{})
	// TODO(SI): What error to use here?
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve side input: %w", err)
	}
	return resp, nil
}
