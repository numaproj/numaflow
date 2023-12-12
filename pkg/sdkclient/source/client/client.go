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

package client

import (
	"context"
	"fmt"
	"io"
	"log"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt sourcepb.SourceClient
}

var _ Client = (*client)(nil)

// New creates a new client object.
func New(inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.SourceAddr)

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	// Wait for server info to be ready
	serverInfo, err := util.WaitForServerInfo(opts.ServerInfoReadinessTimeout(), opts.ServerInfoFilePath())
	if err != nil {
		return nil, err
	}

	if serverInfo != nil {
		log.Printf("ServerInfo: %v\n", serverInfo)
	}

	// connect to the grpc server
	c := new(client)
	sockAddr := fmt.Sprintf("%s:%s", sdkclient.UDS, opts.UdsSockAddr())
	conn, err := grpc.Dial(sockAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.MaxMessageSize()),
			grpc.MaxCallSendMsgSize(opts.MaxMessageSize())))
	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}
	c.conn = conn
	c.grpcClt = sourcepb.NewSourceClient(conn)
	return c, nil
}

// NewFromClient creates a new client object from the grpc client. This is used for testing.
func NewFromClient(c sourcepb.SourceClient) (Client, error) {
	return &client{grpcClt: c}, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(ctx context.Context) error {
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

// ReadFn reads data from the source.
func (c *client) ReadFn(ctx context.Context, req *sourcepb.ReadRequest, datumCh chan<- *sourcepb.ReadResponse) error {
	stream, err := c.grpcClt.ReadFn(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to execute c.grpcClt.ReadFn(): %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var resp *sourcepb.ReadResponse
			resp, err = stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			datumCh <- resp
		}
	}
}

// AckFn acknowledges the data from the source.
func (c *client) AckFn(ctx context.Context, req *sourcepb.AckRequest) (*sourcepb.AckResponse, error) {
	return c.grpcClt.AckFn(ctx, req)
}

// PendingFn returns the number of pending data from the source.
func (c *client) PendingFn(ctx context.Context, req *emptypb.Empty) (*sourcepb.PendingResponse, error) {
	return c.grpcClt.PendingFn(ctx, req)
}

// PartitionsFn returns the number of partitions from the source.
func (c *client) PartitionsFn(ctx context.Context, req *emptypb.Empty) (*sourcepb.PartitionsResponse, error) {
	return c.grpcClt.PartitionsFn(ctx, req)
}
