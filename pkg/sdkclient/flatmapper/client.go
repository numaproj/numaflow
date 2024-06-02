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

package flatmapper

import (
	"context"
	"errors"
	"io"
	"log"

	flatmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/flatmap/v1"
	"github.com/numaproj/numaflow-go/pkg/info"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	grpcutil "github.com/numaproj/numaflow/pkg/sdkclient/grpc"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt flatmappb.FlatmapClient
}

func (c client) MapFn(ctx context.Context, datumStreamCh <-chan *flatmappb.MapRequest) (<-chan *flatmappb.MapResponse, <-chan error) {
	var (
		errCh      = make(chan error)
		responseCh = make(chan *flatmappb.MapResponse)
	)

	log.Println("MYDEBUG: WHATS HAPPENING", len(datumStreamCh), datumStreamCh)

	// stream the messages to server
	stream, err := c.grpcClt.MapFn(ctx)

	// read the response from the server stream and send it to responseCh channel
	// any error is sent to errCh channel
	go func() {
		defer close(responseCh)
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				var resp *flatmappb.MapResponse
				resp, err = stream.Recv()
				//if err == io.EOF {
				//	return
				//}
				if errors.Is(err, io.EOF) {
					// skip selection on nil channel
					//errCh = nil
					//close(responseCh)
					return
				}
				errSDK := sdkerr.ToUDFErr("c.grpcClt.MapStreamFn", err)
				if errSDK != nil {
					log.Println("MYDEBUG: ERROR in recv", err, errSDK)
					errCh <- errSDK
					return
				}
				log.Println("MYDEBUG: GOT IT FROM GRPC", resp.Result.Uuid)
				responseCh <- resp
			}
		}
	}()
	// Read from the inputStream and send messages
	for inputMsg := range datumStreamCh {
		log.Println("MYDEBUG: Sending to grpc", inputMsg.Uuid)
		err = stream.Send(inputMsg)
		if err != nil {
			go func(sErr error) {
				errCh <- sdkerr.ToUDFErr("c.grpcClt.MapFn", sErr)
			}(err)
			break
		}
		//if err != nil {
		//	errCh <- sdkerr.ToUDFErr("MapFn stream.Send()", sendErr)
		//	return
		//}
	}
	sendErr := stream.CloseSend()
	if sendErr != nil && !errors.Is(sendErr, io.EOF) {
		go func(sErr error) {
			errCh <- sdkerr.ToUDFErr("c.grpcClt.MapFn stream.CloseSend()", sErr)
		}(sendErr)
	}

	return responseCh, errCh
}

func (c client) CloseConn(ctx context.Context) error {
	return c.conn.Close()
}

func (c client) IsReady(ctx context.Context, in *emptypb.Empty) (bool, error) {
	resp, err := c.grpcClt.IsReady(ctx, in)
	if err != nil {
		return false, err
	}
	return resp.GetReady(), nil
}

// New creates a new client object.
func New(serverInfo *info.ServerInfo, inputOptions ...sdkclient.Option) (Client, error) {
	var opts = sdkclient.DefaultOptions(sdkclient.FlatmapAddr)

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
	c.grpcClt = flatmappb.NewFlatmapClient(conn)
	return c, nil
}
