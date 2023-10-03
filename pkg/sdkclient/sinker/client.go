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
	"log"
	"time"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt sinkpb.SinkClient
}

var _ Client = (*client)(nil)

// New creates a new client object.
func New(inputOptions ...Option) (Client, error) {
	var opts = &options{
		sockAddr:                   sdkclient.SinkAddr,
		serverInfoFilePath:         sdkclient.ServerInfoFilePath,
		serverInfoReadinessTimeout: 120 * time.Second,                   // Default timeout is 120 seconds
		maxMessageSize:             sdkclient.DefaultGRPCMaxMessageSize, // 64 MB
	}

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	// Wait for server info to be ready
	serverInfo, err := util.WaitForServerInfo(opts.serverInfoReadinessTimeout, opts.serverInfoFilePath)
	if err != nil {
		return nil, err
	}

	if serverInfo != nil {
		log.Printf("ServerInfo: %v\n", serverInfo)
	}

	// connect to the server
	c := new(client)
	sockAddr := fmt.Sprintf("%s:%s", sdkclient.UDS, opts.sockAddr)
	conn, err := grpc.Dial(sockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.maxMessageSize), grpc.MaxCallSendMsgSize(opts.maxMessageSize)))
	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}

	c.conn = conn
	c.grpcClt = sinkpb.NewSinkClient(conn)
	return c, nil
}

// NewFromClient creates a new client object from a grpc client, which is useful for testing.
func NewFromClient(c sinkpb.SinkClient) (Client, error) {
	return &client{grpcClt: c}, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(ctx context.Context) error {
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

// SinkFn applies a function to a list of requests.
func (c *client) SinkFn(ctx context.Context, requests []*sinkpb.SinkRequest) (*sinkpb.SinkResponse, error) {
	stream, err := c.grpcClt.SinkFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute c.grpcClt.SinkFn(): %w", err)
	}
	for _, datum := range requests {
		if err := stream.Send(datum); err != nil {
			return nil, fmt.Errorf("failed to execute stream.Send(%v): %w", datum, err)
		}
	}
	responseList, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("failed to execute stream.CloseAndRecv(): %w", err)
	}

	return responseList, nil
}
