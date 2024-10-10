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

package mapper

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerror "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn         *grpc.ClientConn
	grpcClt      mappb.MapClient
	stream       mappb.Map_MapFnClient
	batchMapMode bool
}

// New creates a new client object.
func New(ctx context.Context, serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.MapAddr)

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
	c.grpcClt = mappb.NewMapClient(conn)
	c.batchMapMode = opts.BatchMapMode()

	var logger = logging.FromContext(ctx)

waitUntilReady:
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for mapper gRPC server to be ready: %w", ctx.Err())
		default:
			_, err := c.IsReady(ctx, &emptypb.Empty{})
			if err != nil {
				logger.Warnf("Mapper server is not ready: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue waitUntilReady
			}
			break waitUntilReady
		}
	}

	c.stream, err = c.grpcClt.MapFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create a gRPC stream for map: %w", err)
	}

	if err := doHandshake(c.stream); err != nil {
		return nil, err
	}

	return c, nil
}

func doHandshake(stream mappb.Map_MapFnClient) error {
	// Send handshake request
	handshakeReq := &mappb.MapRequest{
		Handshake: &mappb.Handshake{
			Sot: true,
		},
	}
	if err := stream.Send(handshakeReq); err != nil {
		return fmt.Errorf("failed to send handshake request for map: %w", err)
	}

	handshakeResp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive handshake response from map stream: %w", err)
	}
	if resp := handshakeResp.GetHandshake(); resp == nil || !resp.GetSot() {
		return fmt.Errorf("invalid handshake response for map. Received='%+v'", resp)
	}
	return nil
}

// NewFromClient creates a new client object from a grpc client. This is used for testing.
func NewFromClient(ctx context.Context, c mappb.MapClient) (Client, error) {
	stream, err := c.MapFn(ctx)
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
func (c *client) CloseConn() error {
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

// MapFn applies a function to each map request element.
func (c *client) MapFn(ctx context.Context, requests []*mappb.MapRequest) ([]*mappb.MapResponse, error) {
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
				return sdkerror.ToUDFErr("c.grpcClt.MapFn stream.Send", err)
			}
		}
		// if it is a batch map, we need to send an end of transmission message to the server
		// to indicate that the batch is finished.
		if c.batchMapMode {
			if err := c.stream.Send(&mappb.MapRequest{Status: &mappb.MapRequest_Status{Eot: true}}); err != nil {
				return sdkerror.ToUDFErr("c.grpcClt.MapFn stream.Send end of transmission", err)
			}
		}
		return nil
	})

	// receive n responses
	responses := make([]*mappb.MapResponse, len(requests))
	eg.Go(func() error {
		for i := 0; i < len(requests); i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			resp, err := c.stream.Recv()
			if err != nil {
				return sdkerror.ToUDFErr("c.grpcClt.MapFn stream.Recv", err)
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
