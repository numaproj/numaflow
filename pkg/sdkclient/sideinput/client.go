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

package sideinput

import (
	"context"
	"fmt"
	"time"

	sideinputpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt sideinputpb.SideInputClient
}

var _ Client = (*client)(nil)

// New creates a new client object.
func New(serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (*client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.SideInputAddr)

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	// Connect to the server
	conn, err := grpcutil.ConnectToServer(opts.UdsSockAddr(), serverInfo, opts.MaxMessageSize())
	if err != nil {
		return nil, err
	}

	c := new(client)
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
