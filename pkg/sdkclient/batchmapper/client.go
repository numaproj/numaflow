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

	batchmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/batchmap/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt batchmappb.BatchMapClient
}

// New creates a new client object.
func New(serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.BatchMapAddr)

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
	c.grpcClt = batchmappb.NewBatchMapClient(conn)
	return c, nil
}

func NewFromClient(c batchmappb.BatchMapClient) (Client, error) {
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

// BatchMapFn is the handler for the gRPC client (Numa container)
// It takes in a stream of input Requests, sends them to the gRPC server(UDF) and then streams the
// responses received back on a channel asynchronously.
// We spawn 2 goroutines here, one for sending the requests over the stream
// and the other one for receiving the responses
func (c *client) BatchMapFn(ctx context.Context, inputCh <-chan *batchmappb.BatchMapRequest) (<-chan *batchmappb.BatchMapResponse, <-chan error) {
	// errCh is used to track and propagate any errors that might occur during the rpc lifecyle, these could include
	// errors in sending, UDF errors etc
	// These are propagated to the applier for further handling
	errCh := make(chan error)

	// response channel for streaming back the results received from the gRPC server
	responseCh := make(chan *batchmappb.BatchMapResponse)

	// BatchMapFn is a bidirectional streaming RPC
	// We get a Map_BatchMapFnClient object over which we can send the requests,
	// receive the responses asynchronously.
	// TODO(map-batch): this creates a new gRPC stream for every batch,
	// it might be useful to see the performance difference between this approach
	// and a long-running RPC
	stream, err := c.grpcClt.BatchMapFn(ctx)
	if err != nil {
		go func() {
			errCh <- sdkerr.ToUDFErr("c.grpcClt.BatchMapFn", err)
		}()
		// passing a nil channel for responseCh to ensure that it is never selected as this is an error scenario
		// and we should be reading on the error channel only.
		return nil, errCh
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
		for {
			resp, recvErr = stream.Recv()
			// check if this is EOF error, which indicates that no more responses left to process on the
			// stream from the UDF, in such a case we return without any error to indicate this
			if errors.Is(recvErr, io.EOF) {
				// set the error channel to nil in case of no errors to ensure
				// that it is not picked up
				errCh = nil
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
		}
	}()

	// Read from the read messages and send them individually to the bi-di stream for processing
	// in case there is an error in sending, send it to the error channel for handling
	go func() {
		for {
			select {
			case <-ctx.Done():
				// If the context is done we do not want to send further on the stream,
				// the Recv should get an error from the server as the stream uses the same ctx
				return
			case inputMsg, ok := <-inputCh:
				// If there are no more messages left to read on the channel, then we can
				// close the stream.
				if !ok {
					// CloseSend closes the send direction of the stream. This indicates to the
					// UDF that we have sent all requests from the client, and it can safely
					// stop listening on the stream
					sendErr := stream.CloseSend()
					if sendErr != nil && !errors.Is(sendErr, io.EOF) {
						errCh <- sdkerr.ToUDFErr("c.grpcClt.BatchMapFn stream.CloseSend()", sendErr)
					}
					// exit this routine
					return
				} else {
					err = stream.Send(inputMsg)
					if err != nil {
						errCh <- sdkerr.ToUDFErr("c.grpcClt.BatchMapFn", err)
						// On an error we would be stopping any further processing and go for a replay
						// so return directly
						return
					}
				}
			}
		}

	}()
	return responseCh, errCh

}
