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

package accumulator

import (
	"context"
	"fmt"
	"io"
	"time"

	accumulatorpb "github.com/numaproj/numaflow-go/pkg/apis/proto/accumulator/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerror "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt accumulatorpb.AccumulatorClient
	stream  accumulatorpb.Accumulator_AccumulateFnClient
	log     *zap.SugaredLogger
}

// New creates a new client object.
func New(ctx context.Context, serverInfo *serverinfo.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.AccumulatorAddr)

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
	c.grpcClt = accumulatorpb.NewAccumulatorClient(conn)

	c.log = logging.FromContext(ctx)

waitUntilReady:
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for accumulator gRPC server to be ready: %w", ctx.Err())
		default:
			_, err := c.IsReady(ctx, &emptypb.Empty{})
			if err != nil {
				c.log.Warnf("Accumulator server is not ready: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue waitUntilReady
			}
			break waitUntilReady
		}
	}

	return c, nil
}

// NewFromClient creates a new client object from a grpc client. This is used for testing.
func NewFromClient(ctx context.Context, c accumulatorpb.AccumulatorClient) (Client, error) {
	stream, err := c.AccumulateFn(ctx)
	if err != nil {
		return nil, err
	}

	return &client{
		grpcClt: c,
		stream:  stream,
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

// AccumulatorFn applies accumulateFn function to a datum stream asynchronously.
func (c *client) AccumulatorFn(ctx context.Context, datumStreamCh <-chan *accumulatorpb.AccumulatorRequest) (<-chan *accumulatorpb.AccumulatorResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *accumulatorpb.AccumulatorResponse)
	)

	// stream the messages to server
	stream, err := c.grpcClt.AccumulateFn(ctx)

	if err != nil {
		go func() {
			errCh <- sdkerror.ToUDFErr("c.grpcClt.AccumulatorFn", err)
		}()
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
				if sendErr = stream.Send(datum); sendErr != nil {
					errCh <- sdkerror.ToUDFErr("AccumulatorFn stream.Send()", sendErr)
				}
			}
		}
		// close the stream after sending all the messages
		sendErr = stream.CloseSend()
		if sendErr != nil {
			errCh <- sdkerror.ToUDFErr("AccumulatorFn stream.Send()", sendErr)
		}
	}()

	// read the response from the server stream and send it to responseCh channel
	// any error is sent to errCh channel
	go func() {
		var resp *accumulatorpb.AccumulatorResponse
		var recvErr error
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				resp, recvErr = stream.Recv()
				// if the stream is closed, close the responseCh and errCh channels and return
				if recvErr == io.EOF {
					close(responseCh)
					return
				}
				if recvErr != nil {
					errCh <- sdkerror.ToUDFErr("AccumulatorFn stream.Recv()", recvErr)
				}
				responseCh <- resp
			}
		}
	}()

	return responseCh, errCh
}
