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

package batchmapper

import (
	"context"
	"errors"
	"io"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	batchmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"github.com/numaproj/numaflow-go/pkg/info"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt batchmappb.MapClient
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
	c.grpcClt = batchmappb.NewMapClient(conn)
	return c, nil
}

func NewFromClient(c batchmappb.MapClient) (Client, error) {
	return &client{
		grpcClt: c,
	}, nil
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

// BatchMapFn
func (c *client) BatchMapFn(ctx context.Context, inputCh <-chan *batchmappb.MapRequest) (<-chan *batchmappb.BatchMapResponse, <-chan error) {
	errCh := make(chan error)
	responseCh := make(chan *batchmappb.BatchMapResponse)
	stream, err := c.grpcClt.BatchMapFn(ctx)
	if err != nil {
		go func() {
			errCh <- sdkerr.ToUDFErr("c.grpcClt.BatchMapFn stream", err)
		}()
		return responseCh, errCh
	}

	// read the response from the server stream and send it to responseCh channel
	// any error is sent to errCh channel
	go func() {
		// close this channel to indicate that no more elements left to receive from grpc
		// We do defer here on the whole go-routine as even during a error scenario, we
		// want to close the channel and stop forwarding any more responses from the UDF
		// as we would be replaying the current ones.
		defer close(responseCh)
		var resp *batchmappb.BatchMapResponse
		var recvErr error
		index := 0
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				resp, recvErr = stream.Recv()
				// check if this is EOF error, which indicates that no more responses left to process on the
				// stream from the UDF, in such a case we return without any error to indicate this
				if errors.Is(recvErr, io.EOF) {
					log.Println("MYDEBUG: GOT EOF FROM STREAM ", index)
					return
				}
				// If this is some other error, propagate it to error channel,
				// also close the response channel(done using the defer close) to indicate no more messages being read
				errSDK := sdkerr.ToUDFErr("c.grpcClt.BatchMapFn", recvErr)
				if errSDK != nil {
					errCh <- errSDK
					return
				}
				// send the response upstream
				responseCh <- resp
				index += 1
			}
		}
	}()

	// Read from the read messages and send them individually to the bi-di stream for processing
	// in case there is an error in sending, send it to the error channel for handling
	go func() {
		for inputMsg := range inputCh {
			err = stream.Send(inputMsg)
			if err != nil {
				go func(sErr error) {
					errCh <- sdkerr.ToUDFErr("c.grpcClt.BatchMapFn", sErr)
				}(err)
				break
			}
		}
		// CloseSend closes the send direction of the stream. This indicates to the
		// UDF that we have sent all requests from the client, and it can safely
		// stop listening on the stream
		sendErr := stream.CloseSend()
		if sendErr != nil && !errors.Is(sendErr, io.EOF) {
			go func(sErr error) {
				errCh <- sdkerr.ToUDFErr("c.grpcClt.BatchMapFn stream.CloseSend()", sErr)
			}(sendErr)
		}
	}()
	return responseCh, errCh

}
