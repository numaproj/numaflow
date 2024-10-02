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

package sourcetransformer

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	transformpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt transformpb.SourceTransformClient
	stream  transformpb.SourceTransform_SourceTransformFnClient
}

// New creates a new client object.
func New(ctx context.Context, serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.SourceTransformerAddr)

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
	c.grpcClt = transformpb.NewSourceTransformClient(conn)

	var logger = logging.FromContext(ctx)

waitUntilReady:
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for transformer gRPC server to be ready: %w", ctx.Err())
		default:
			_, err := c.IsReady(ctx, &emptypb.Empty{})
			if err != nil {
				logger.Warnf("Transformer server is not ready: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue waitUntilReady
			}
			break waitUntilReady
		}
	}

	c.stream, err = c.grpcClt.SourceTransformFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create a gRPC stream for source transform: %w", err)
	}

	if err := doHandshake(c.stream); err != nil {
		return nil, err
	}

	return c, nil
}

func doHandshake(stream transformpb.SourceTransform_SourceTransformFnClient) error {
	// Send handshake request
	handshakeReq := &transformpb.SourceTransformRequest{
		Handshake: &transformpb.Handshake{
			Sot: true,
		},
	}
	if err := stream.Send(handshakeReq); err != nil {
		return fmt.Errorf("failed to send handshake request for source tansform: %w", err)
	}

	handshakeResp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive handshake response from source transform stream: %w", err)
	}
	if resp := handshakeResp.GetHandshake(); resp == nil || !resp.GetSot() {
		return fmt.Errorf("invalid handshake response for source transform. Received='%+v'", resp)
	}
	return nil
}

// NewFromClient creates a new client object from a grpc client. This is used for testing.
func NewFromClient(ctx context.Context, c transformpb.SourceTransformClient) (Client, error) {
	stream, err := c.SourceTransformFn(ctx)
	if err != nil {
		return nil, err
	}

	if err := doHandshake(stream); err != nil {
		return nil, err
	}

	return &client{
		grpcClt: c,
		stream:  stream,
	}, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(_ context.Context) error {
	err := c.stream.CloseSend()
	if err != nil {
		return err
	}
	if c.conn == nil {
		return nil
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

// SourceTransformFn SourceTransformerFn applies a function to each request element.
// Response channel will not be closed. Caller can select on response and error channel to exit on first error.
func (c *client) SourceTransformFn(ctx context.Context, requests []*transformpb.SourceTransformRequest) ([]*transformpb.SourceTransformResponse, error) {
	var eg errgroup.Group
	// send n requests
	eg.Go(func() error {
		for _, req := range requests {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := c.stream.Send(req); err != nil {
				return sdkerr.ToUDFErr("c.grpcClt.SourceTransformFn stream.Send", err)
			}
		}
		return nil
	})

	// receive n responses
	responses := make([]*transformpb.SourceTransformResponse, len(requests))
	eg.Go(func() error {
		for i := 0; i < len(requests); i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			resp, err := c.stream.Recv()
			if err != nil {
				return sdkerr.ToUDFErr("c.grpcClt.SourceTransformFn stream.Recv", err)
			}
			responses[i] = resp
		}
		return nil
	})

	// wait for the send and receive goroutines to finish
	// if any of the goroutines return an error, the error will be caught here
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return responses, nil
}
