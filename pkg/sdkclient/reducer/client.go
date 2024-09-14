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
	"errors"
	"io"

	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
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
	grpcClt reducepb.ReduceClient
}

// New creates a new client object.
func New(serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.ReduceAddr)

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
		go func(sErr error) {
			errCh <- sdkerr.ToUDFErr("c.grpcClt.ReduceFn", sErr)
		}(err)
	}

	// read from the datumStreamCh channel and send it to the server stream
	go func() {
		var sendErr error
	outerLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case datum, ok := <-datumStreamCh:
				if !ok {
					break outerLoop
				}
				// TODO: figure out why send is getting EOF (could be because the client has already handled SIGTERM)
				if sendErr = stream.Send(datum); sendErr != nil && !errors.Is(sendErr, io.EOF) {
					errCh <- sdkerr.ToUDFErr("ReduceFn stream.Send()", sendErr)
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
			sendErr = stream.CloseSend()
			if sendErr != nil && !errors.Is(sendErr, io.EOF) {
				errCh <- sdkerr.ToUDFErr("ReduceFn stream.CloseSend()", sendErr)
			}
		}
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
				// if the stream is closed, close the responseCh return
				if errors.Is(recvErr, io.EOF) {
					// nil channel will never be selected
					errCh = nil
					close(responseCh)
					return
				}
				if recvErr != nil {
					errCh <- sdkerr.ToUDFErr("ReduceFn stream.Recv()", recvErr)
				}
				responseCh <- resp
			}
		}
	}()

	return responseCh, errCh
}
