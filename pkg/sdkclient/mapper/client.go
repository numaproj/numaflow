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
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"github.com/numaproj/numaflow-go/pkg/info"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerror "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt mappb.MapClient
}

// New creates a new client object.
func New(serverInfo *info.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
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
	return c, nil
}

// NewFromClient creates a new client object from a grpc client. This is used for testing.
func NewFromClient(c mappb.MapClient) (Client, error) {
	return &client{
		grpcClt: c,
	}, nil
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

// MapFn applies a function to each datum element.
func (c *client) MapFn(ctx context.Context, request *mappb.MapRequest) (*mappb.MapResponse, error) {
	mapResponse, err := c.grpcClt.MapFn(ctx, request)
	//mapResponse, err := TestMap(ctx, request)
	err = sdkerror.ToUDFErr("c.grpcClt.MapFn", err)
	if err != nil {
		return nil, err
	}
	return mapResponse, nil
}

func TestMap(ctx context.Context, msg *mappb.MapRequest) (*mappb.MapResponse, error) {
	val := msg.GetValue()
	var elements []*mappb.MapResponse_Result
	strs := strings.Split(string(val), ",")
	for _, x := range strs {
		elements = append(elements, &mappb.MapResponse_Result{
			Keys:  msg.GetKeys(),
			Value: []byte(x),
			Tags:  msg.GetKeys(),
		})
	}
	res := &mappb.MapResponse{
		Results: elements,
	}
	return res, nil
}
