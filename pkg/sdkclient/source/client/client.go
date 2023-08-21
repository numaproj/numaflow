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
	"time"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"github.com/numaproj/numaflow-go/pkg/info"
	"github.com/numaproj/numaflow-go/pkg/source"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt sourcepb.SourceClient
}

var _ Client = (*client)(nil)

// New creates a new client object.
func New(inputOptions ...Option) (*client, error) {
	var opts = &options{
		sockAddr:                   source.UdsAddr,
		serverInfoFilePath:         info.ServerInfoFilePath,
		serverInfoReadinessTimeout: 120 * time.Second, // Default timeout is 120 seconds
		maxMessageSize:             1024 * 1024 * 64,  // 64 MB
	}

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.serverInfoReadinessTimeout)
	defer cancel()

	if err := info.WaitUntilReady(ctx, info.WithServerInfoFilePath(opts.serverInfoFilePath)); err != nil {
		return nil, fmt.Errorf("failed to wait until server info is ready: %w", err)
	}

	serverInfo, err := info.Read(info.WithServerInfoFilePath(opts.serverInfoFilePath))
	if err != nil {
		return nil, fmt.Errorf("failed to read server info: %w", err)
	}
	// TODO: Use serverInfo to check compatibility.
	if serverInfo != nil {
		log.Printf("ServerInfo: %v\n", serverInfo)
	}

	c := new(client)
	sockAddr := fmt.Sprintf("%s:%s", source.UDS, opts.sockAddr)
	conn, err := grpc.Dial(sockAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.maxMessageSize),
			grpc.MaxCallSendMsgSize(opts.maxMessageSize)))
	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}
	c.conn = conn
	c.grpcClt = sourcepb.NewSourceClient(conn)
	return c, nil
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
	defer close(datumCh)
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
