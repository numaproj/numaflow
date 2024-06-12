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
	"strconv"
	"strings"

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

// MapFn is the RPC handler for the gRPC client (Numa container)
// It takes in a stream of input Requests, sends them to the gRPC server(UDF) and then streams the
// responses received back on a channel asynchronously.
// We spawn 2 goroutines here, one for sending the requests over the stream
// and the other one for receiving the responses
func (c client) MapFn(ctx context.Context, datumStreamCh <-chan *flatmappb.MapRequest) (<-chan *flatmappb.MapResponse, <-chan error) {
	var (
		// errCh is used to track and propagate any errors that might occur during the rpc lifecyle, these could include
		// errors in sending, UDF errors etc
		// These are propagated to the applier for further handling
		errCh = make(chan error)
		// TODO(stream): Should we keep this buffered? Might help with error scenario to drain any
		//  messages already processed
		responseCh = make(chan *flatmappb.MapResponse)
	)

	//MapFn is a bidirectional RPC
	//We get a Flatmap_MapFnClient interface over which we can send the requests,
	//receive the responses asynchronously.
	//TODO(stream): this creates a new gRPC stream for every batch,
	//it might be useful to see the performance difference between this approach
	//and a long-running RPC
	stream, err := c.grpcClt.MapFn(ctx)

	// If any initial error, send it to the error channel
	if err != nil {
		go func(sErr error) {
			errCh <- sdkerr.ToUDFErr("c.grpcClt.MapFn", sErr)
		}(err)
	}

	// Response routine:
	// read the response from the server stream and send it to responseCh channel
	// any error is sent to errCh channel
	go func() {
		// close this channel to indicate that no more elements left to receive from grpc
		// We do defer here on the whole go-routine as even during a error scenario, we
		// want to close the channel and stop forwarding any more responses from the UDF
		// as we would be replaying the current ones.
		defer close(responseCh)
		for {
			select {
			// In case of the context done, return the error and stop further processing
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				var resp *flatmappb.MapResponse
				resp, err := stream.Recv()
				// check if this is EOF error, which indicates that no more responses left to process on the
				// stream from the UDF, in such a case we return without any error to indicate this
				if errors.Is(err, io.EOF) {
					return
				}
				// If this is some other error, propagate it to error channel,
				// also close the response channel(done using the defer close) to indicate no more messages being read
				errSDK := sdkerr.ToUDFErr("flatmap c.grpcClt.MapFn", err)
				if errSDK != nil {
					errCh <- errSDK
					return
				}
				//log.Print("MYDEBUG GOT FROM grpc ", resp.Result.GetUuid())
				responseCh <- resp
			}
		}
	}()
	// Read from the read messages and send them individually to the bi-di stream for processing
	// in case there is an error in sending, send it to the error channel for handling
	go func() {
		for inputMsg := range datumStreamCh {
			err := stream.Send(inputMsg)
			if err != nil {
				go func(sErr error) {
					errCh <- sdkerr.ToUDFErr("c.grpcClt.MapFn", sErr)
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
				errCh <- sdkerr.ToUDFErr("flatmap c.grpcClt.MapFn stream.CloseSend()", sErr)
			}(sendErr)
		}
	}()
	return responseCh, errCh

	// FOR BYPASSING GRPC
	// Read from the read messages and send them individually to the bi-di stream for processing
	// in case there is an error in sending, send it to the error channel for handling
	//go func() {
	//	defer close(responseCh)
	//	//var wg sync.WaitGroup
	//	//for i := 0; i < 1000; i++ {
	//	//	wg.Add(1)
	//	//	go func() {
	//	//		go TestMap(ctx, datumStreamCh, responseCh, &wg)
	//	//	}()
	//	//}
	//	//wg.Wait()
	//	for inputMsg := range datumStreamCh {
	//		mapResp := TestMap(ctx, inputMsg)
	//		for _, resp := range mapResp {
	//			responseCh <- resp
	//		}
	//	}
	//}()
	//return responseCh, errCh
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

func TestMap(ctx context.Context, msg *flatmappb.MapRequest) []*flatmappb.MapResponse {
	val := msg.GetValue()
	var elements []*flatmappb.MapResponse
	strs := strings.Split(string(val), ",")
	for idx, x := range strs {
		elements = append(elements, &flatmappb.MapResponse{
			Result: &flatmappb.MapResponse_Result{
				Keys:  msg.GetKeys(),
				Value: []byte(x),
				Tags:  msg.GetKeys(),
				EOR:   false,
				Uuid:  msg.GetUuid(),
				Index: strconv.Itoa(idx),
				Total: int32(len(strs)),
			},
		})
		//element := &flatmappb.MapResponse{
		//	Result: &flatmappb.MapResponse_Result{
		//		Keys:  msg.GetKeys(),
		//		Value: []byte(x),
		//		Tags:  msg.GetKeys(),
		//		EOR:   false,
		//		Uuid:  msg.GetUuid(),
		//		Index: strconv.Itoa(idx),
		//		Total: int32(len(strs)),
		//	},
		//}
		//responseChan <- element
	}
	if len(strs) == 0 {
		// Append the EOR to indicate that the processing for the given request has completed
		elements = append(elements, &flatmappb.MapResponse{
			Result: &flatmappb.MapResponse_Result{
				EOR:   true,
				Uuid:  msg.GetUuid(),
				Total: int32(len(strs)),
			},
		})
		//element := &flatmappb.MapResponse{
		//	Result: &flatmappb.MapResponse_Result{
		//		EOR:   true,
		//		Uuid:  msg.GetUuid(),
		//		Total: int32(len(strs)),
		//	},
		//}
		//responseChan <- element
	}
	return elements
}
