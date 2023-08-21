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

package clienttest

import (
	"context"
	"fmt"
	"io"

	sourcesdk "github.com/numaproj/numaflow/pkg/sdkclient/source/client"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1/sourcemock"
	"google.golang.org/protobuf/types/known/emptypb"
)

// client contains the grpc client for testing.
type client struct {
	grpcClt sourcepb.SourceClient
}

var _ sourcesdk.Client = (*client)(nil)

// New creates a new mock client object.
func New(c *sourcemock.MockSourceClient) (*client, error) {
	return &client{c}, nil
}

// CloseConn closes the grpc client connection.
func (c *client) CloseConn(ctx context.Context) error {
	return nil
}

// IsReady returns true if the grpc connection is ready to use.
func (c *client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

func (c *client) ReadFn(ctx context.Context, req *sourcepb.ReadRequest, datumCh chan<- *sourcepb.ReadResponse) error {
	defer close(datumCh)
	stream, err := c.grpcClt.ReadFn(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to execute c.grpcClt.MapStreamFn(): %w", err)
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

func (c *client) AckFn(ctx context.Context, req *sourcepb.AckRequest) (*sourcepb.AckResponse, error) {
	return c.grpcClt.AckFn(ctx, req)
}

func (c *client) PendingFn(ctx context.Context, req *emptypb.Empty) (*sourcepb.PendingResponse, error) {
	return c.grpcClt.PendingFn(ctx, req)
}
