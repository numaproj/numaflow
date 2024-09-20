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

package source

import (
	"context"
	"errors"
	"fmt"
	"time"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn       *grpc.ClientConn
	grpcClt    sourcepb.SourceClient
	readStream sourcepb.Source_ReadFnClient
	ackStream  sourcepb.Source_AckFnClient
}

var _ Client = (*client)(nil)

func New(ctx context.Context, serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.SourceAddr)
	var logger = logging.FromContext(ctx)
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
	c.grpcClt = sourcepb.NewSourceClient(conn)

	// wait until the server is ready
waitUntilReady:
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("failed to connect to the server: %v", ctx.Err())
		default:
			ready, _ := c.IsReady(ctx, &emptypb.Empty{})
			if ready {
				break waitUntilReady
			} else {
				logger.Warnw("source client is not ready")
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	c.readStream, err = c.grpcClt.ReadFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create read stream: %v", err)
	}

	c.ackStream, err = c.grpcClt.AckFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create ack stream: %v", err)
	}

	// Send handshake request for read stream
	readHandshakeRequest := &sourcepb.ReadRequest{
		Handshake: &sourcepb.Handshake{
			Sot: true,
		},
	}
	if err := c.readStream.Send(readHandshakeRequest); err != nil {
		return nil, fmt.Errorf("failed to send read handshake request: %v", err)
	}

	// Wait for handshake response for read stream
	readHandshakeResponse, err := c.readStream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive read handshake response: %v", err)
	}
	if readHandshakeResponse.GetHandshake() == nil || !readHandshakeResponse.GetHandshake().GetSot() {
		return nil, fmt.Errorf("invalid read handshake response")
	}

	// Send handshake request for ack stream
	ackHandshakeRequest := &sourcepb.AckRequest{
		Handshake: &sourcepb.Handshake{
			Sot: true,
		},
	}
	if err := c.ackStream.Send(ackHandshakeRequest); err != nil {
		return nil, fmt.Errorf("failed to send ack handshake request: %v", err)
	}

	// Wait for handshake response for ack stream
	ackHandshakeResponse, err := c.ackStream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive ack handshake response: %v", err)
	}
	if ackHandshakeResponse.GetHandshake() == nil || !ackHandshakeResponse.GetHandshake().GetSot() {
		return nil, fmt.Errorf("invalid ack handshake response")
	}

	return c, nil
}

// NewFromClient creates a new client object from the grpc client. This is used for testing.
func NewFromClient(ctx context.Context, srcClient sourcepb.SourceClient, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.SourceAddr)

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	c := new(client)
	c.grpcClt = srcClient

	c.readStream, _ = c.grpcClt.ReadFn(ctx)
	c.ackStream, _ = c.grpcClt.AckFn(ctx)

	return c, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(context.Context) error {
	err := c.readStream.CloseSend()
	if err != nil {
		return err
	}
	err = c.ackStream.CloseSend()
	if err != nil {
		return err
	}
	return c.conn.Close()
}

// IsReady returns true if the grpc connection is ready to use.
func (c *client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

func (c *client) ReadFn(_ context.Context, req *sourcepb.ReadRequest, datumCh chan<- *sourcepb.ReadResponse) error {
	err := c.readStream.Send(req)
	if err != nil {
		return fmt.Errorf("failed to send read request: %v", err)
	}

	for {
		resp, err := c.readStream.Recv()
		// we don't need an EOF check because we only close the stream during shutdown.
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive read response: %v", err)
		}
		if resp.GetStatus().GetEot() {
			break
		}
		datumCh <- resp
	}
	return nil
}

// AckFn acknowledges the data from the source.
func (c *client) AckFn(_ context.Context, reqs []*sourcepb.AckRequest) (*sourcepb.AckResponse, error) {
	// Send the ack request
	for _, req := range reqs {
		err := c.ackStream.Send(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send ack request: %v", err)
		}
	}

	// Wait for the ack response
	resp, err := c.ackStream.Recv()
	// we don't need an EOF check because we only close the stream during shutdown.
	if err != nil {
		return nil, fmt.Errorf("failed to receive ack response: %v", err)
	}

	return resp, nil
}

// PendingFn returns the number of pending data from the source.
func (c *client) PendingFn(ctx context.Context, req *emptypb.Empty) (*sourcepb.PendingResponse, error) {
	return c.grpcClt.PendingFn(ctx, req)
}

// PartitionsFn returns the number of partitions from the source.
func (c *client) PartitionsFn(ctx context.Context, req *emptypb.Empty) (*sourcepb.PartitionsResponse, error) {
	return c.grpcClt.PartitionsFn(ctx, req)
}
