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

package sinker

import (
	"context"
	"fmt"
	"time"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"go.uber.org/zap"
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
	grpcClt    sinkpb.SinkClient
	sinkStream sinkpb.Sink_SinkFnClient
}

var _ Client = (*client)(nil)

func New(ctx context.Context, serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.SinkAddr)
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
	c.grpcClt = sinkpb.NewSinkClient(conn)

	// Wait until the server is ready
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
				logger.Warnw("waiting for the server to be ready", zap.String("server", opts.UdsSockAddr()))
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	// Create the sink stream
	c.sinkStream, err = c.grpcClt.SinkFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink stream: %v", err)
	}

	// Perform handshake
	handshakeRequest := &sinkpb.SinkRequest{
		Handshake: &sinkpb.Handshake{
			Sot: true,
		},
	}
	if err := c.sinkStream.Send(handshakeRequest); err != nil {
		return nil, fmt.Errorf("failed to send handshake request: %v", err)
	}

	handshakeResponse, err := c.sinkStream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive handshake response: %v", err)
	}
	if handshakeResponse.GetHandshake() == nil || !handshakeResponse.GetHandshake().GetSot() {
		return nil, fmt.Errorf("invalid handshake response")
	}

	return c, nil
}

// NewFromClient creates a new client object from a grpc client, which is useful for testing.
func NewFromClient(ctx context.Context, sinkClient sinkpb.SinkClient, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.SinkAddr)
	var err error

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	c := new(client)
	c.grpcClt = sinkClient

	// Create the sink stream
	c.sinkStream, err = c.grpcClt.SinkFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink stream: %v", err)
	}

	return c, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(ctx context.Context) error {
	if c.conn == nil {
		return nil
	}
	err := c.sinkStream.CloseSend()
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

// SinkFn applies a function to a list of requests.
func (c *client) SinkFn(ctx context.Context, requests []*sinkpb.SinkRequest) ([]*sinkpb.SinkResponse, error) {
	// Stream the array of sink requests
	for _, req := range requests {
		if err := c.sinkStream.Send(req); err != nil {
			return nil, fmt.Errorf("failed to send sink request: %v", err)
		}
	}

	// Wait for the corresponding responses
	var responses []*sinkpb.SinkResponse
	for i := 0; i < len(requests); i++ {
		resp, err := c.sinkStream.Recv()
		if err != nil {
			return nil, fmt.Errorf("failed to receive sink response: %v", err)
		}
		responses = append(responses, resp)
	}

	return responses, nil
}
