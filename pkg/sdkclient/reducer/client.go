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

package reducer

import (
	"context"
	"io"
	"log"

	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt reducepb.ReduceClient
}

// New creates a new client object.
func New(inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.ReduceAddr)

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

	// Connect to the server
	conn, err := util.ConnectToServer(opts.UdsSockAddr(), serverInfo, opts.MaxMessageSize())
	if err != nil {
		return nil, err
	}

	c := new(client)
	c.conn = conn
	c.grpcClt = reducepb.NewReduceClient(conn)
	return c, nil
}

func NewFromClient(c reducepb.ReduceClient) (Client, error) {
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

// ReduceFn applies a reduce function to a datum stream asynchronously.
func (c *client) ReduceFn(ctx context.Context, datumStreamCh <-chan *reducepb.ReduceRequest) (<-chan *reducepb.ReduceResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *reducepb.ReduceResponse)
	)

	// stream the messages to server
	stream, err := c.grpcClt.ReduceFn(ctx)

	if err != nil {
		go func() {
			errCh <- util.ToUDFErr("c.grpcClt.ReduceFn", err)
		}()
	}

	// read from the datumStreamCh channel and send it to the server stream
	go func() {
		var sendErr error
	outerLoop:
		for {
			select {
			case <-ctx.Done():
				print("kerantest - i am returning without closing the send stream.")
				return
			case datum, ok := <-datumStreamCh:
				if !ok {
					break outerLoop
				}
				if sendErr = stream.Send(datum); sendErr != nil {
					errCh <- util.ToUDFErr("ReduceFn stream.Send()", sendErr)
				}
			}
		}
		// close the stream after sending all the messages
		print("kerantest - i am closing the send stream.")
		sendErr = stream.CloseSend()
		if sendErr != nil {
			errCh <- util.ToUDFErr("ReduceFn stream.Send()", sendErr)
		}
		print("kerantest - i am here at the end of the read goroutine.")
	}()

	// read the response from the server stream and send it to responseCh channel
	// any error is sent to errCh channel
	go func() {
		var resp *reducepb.ReduceResponse
		var recvErr error
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				resp, recvErr = stream.Recv()
				print("kerantest - i received a response from the stream.")
				// if the stream is closed, close the responseCh return
				if recvErr == io.EOF {
					print("kerantest - i received a recvErr EOF response from the stream.")
					// nil channel will never be selected
					errCh = nil
					close(responseCh)
					return
				}
				if recvErr != nil {
					errCh <- util.ToUDFErr("ReduceFn stream.Recv()", recvErr)
				}
				responseCh <- resp
			}
		}
	}()

	return responseCh, errCh
}
