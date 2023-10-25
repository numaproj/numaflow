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
	"time"

	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt reducepb.ReduceClient
}

// New creates a new client object.
func New(inputOptions ...Option) (Client, error) {
	var opts = &options{
		maxMessageSize:             sdkclient.DefaultGRPCMaxMessageSize, // 64 MB
		serverInfoFilePath:         sdkclient.ServerInfoFilePath,
		tcpSockAddr:                sdkclient.TcpAddr,
		udsSockAddr:                sdkclient.ReduceAddr,
		serverInfoReadinessTimeout: 120 * time.Second, // Default timeout is 120 seconds
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

	// Connect to the server
	conn, err := util.ConnectToServer(opts.udsSockAddr, opts.tcpSockAddr, serverInfo, opts.maxMessageSize)
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

// ReduceFn applies a reduce function to a datum stream.
func (c *client) ReduceFn(ctx context.Context, datumStreamCh <-chan *reducepb.ReduceRequest) (*reducepb.ReduceResponse, error) {
	var g errgroup.Group
	var finalResponse = &reducepb.ReduceResponse{}

	stream, err := c.grpcClt.ReduceFn(ctx)
	err = util.ToUDFErr("c.grpcClt.ReduceFn", err)
	if err != nil {
		return nil, err
	}
	// stream the messages to server
	g.Go(func() error {
		var sendErr error
		for datum := range datumStreamCh {
			select {
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			default:
				if sendErr = stream.Send(datum); sendErr != nil {
					// we don't need to invoke close on the stream
					// if there is an error gRPC will close the stream.
					return sendErr
				}
			}
		}
		return stream.CloseSend()
	})

	// read the response from the server stream
outputLoop:
	for {
		select {
		case <-ctx.Done():
			return nil, util.ToUDFErr("ReduceFn OutputLoop", status.FromContextError(ctx.Err()).Err())
		default:
			var resp *reducepb.ReduceResponse
			resp, err = stream.Recv()
			if err == io.EOF {
				break outputLoop
			}
			if err != nil {
				err = util.ToUDFErr("ReduceFn stream.Recv()", err)
				return nil, err
			}
			finalResponse.Results = append(finalResponse.Results, resp.Results...)
		}
	}

	err = g.Wait()
	err = util.ToUDFErr("ReduceFn errorGroup", err)
	if err != nil {
		return nil, err
	}

	return finalResponse, nil
}

func (c *client) AsyncReduceFn(ctx context.Context, datumStreamCh <-chan *isb.ReadMessage, id *partition.ID) (<-chan []*isb.WriteMessage, <-chan error) {
	var (
		errCh      = make(chan error, 100)
		responseCh = make(chan []*isb.WriteMessage)
	)

	stream, err := c.grpcClt.ReduceFn(ctx)

	if err != nil {
		go func() {
			select {
			case <-ctx.Done():
				return
			case errCh <- util.ToUDFErr("c.grpcClt.ReduceFn", err):
				return
			}
		}()
	}

	go func() {
		var sendErr error
	outerLoop:
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case datum, ok := <-datumStreamCh:
				if !ok {
					break outerLoop
				}
				if sendErr = stream.Send(createDatum(datum)); sendErr != nil {
					println("got an error while sending to client")
					errCh <- util.ToUDFErr("ReduceFn stream.Send()", sendErr)
				}
			}
		}
		sendErr = stream.CloseSend()
		if sendErr != nil {
			errCh <- util.ToUDFErr("ReduceFn stream.Send()", sendErr)
		}
	}()

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
				if recvErr == io.EOF {
					close(responseCh)
					close(errCh)
					return
				}
				if recvErr != nil {
					errCh <- util.ToUDFErr("ReduceFn stream.Recv()", recvErr)
				}
				responseCh <- convertToWriteMessages(resp, id)
			}
		}
	}()

	return responseCh, errCh
}

func createDatum(readMessage *isb.ReadMessage) *reducepb.ReduceRequest {
	keys := readMessage.Keys
	payload := readMessage.Body.Payload
	parentMessageInfo := readMessage.MessageInfo
	var d = &reducepb.ReduceRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(parentMessageInfo.EventTime),
		Watermark: timestamppb.New(readMessage.Watermark),
	}
	return d
}

func convertToWriteMessages(response *reducepb.ReduceResponse, partitionID *partition.ID) []*isb.WriteMessage {
	taggedMessages := make([]*isb.WriteMessage, 0)
	for _, r := range response.GetResults() {
		keys := r.Keys
		taggedMessage := &isb.WriteMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{
						EventTime: partitionID.End.Add(-1 * time.Millisecond),
						IsLate:    false,
					},
					Keys: keys,
				},
				Body: isb.Body{
					Payload: r.Value,
				},
			},
			Tags: r.Tags,
		}
		taggedMessages = append(taggedMessages, taggedMessage)
	}
	return taggedMessages
}
