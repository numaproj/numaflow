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
	"fmt"
	"io"
	"log"
	"time"

	reducepb "github.com/numaproj/numaflow-go/pkg/apis/proto/reduce/v1"
	"github.com/numaproj/numaflow-go/pkg/shared"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/numaproj/numaflow-go/pkg/info"

	resolver2 "github.com/numaproj/numaflow/pkg/sdkclient/resolver"
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
		maxMessageSize:             1024 * 1024 * 64, // 64 MB
		serverInfoFilePath:         info.ServerInfoFilePath,
		tcpSockAddr:                shared.TcpAddr,
		udsSockAddr:                shared.MapAddr,
		serverInfoReadinessTimeout: 120 * time.Second, // Default timeout is 120 seconds
	}

	for _, inputOption := range inputOptions {
		inputOption(opts)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.serverInfoReadinessTimeout)
	defer cancel()

	if err := info.WaitUntilReady(ctx, info.WithServerInfoFilePath(opts.serverInfoFilePath)); err != nil {
		return nil, fmt.Errorf("failed to wait until server info is ready: %w", err)
	}

	serverInfo, err := info.Read(info.WithServerInfoFilePath(opts.serverInfoFilePath))
	if err != nil {
		return nil, fmt.Errorf("failed to read server info: %w", err)
	}
	// TODO: Use serverInfo to check compatibility.
	if serverInfo != nil {
		log.Printf("ServerInfo: %v\n", serverInfo)
	}

	c := new(client)
	var conn *grpc.ClientConn
	var sockAddr string
	// Make a TCP connection client for multiprocessing grpc server
	if serverInfo.Protocol == info.TCP {
		// Populate connection variables for client connection
		// based on multiprocessing enabled/disabled
		if err := resolver2.RegMultiProcResolver(serverInfo); err != nil {
			return nil, fmt.Errorf("failed to start Multiproc Client: %w", err)
		}

		sockAddr = fmt.Sprintf("%s%s", resolver2.ConnAddr, opts.tcpSockAddr)
		log.Println("Multiprocessing TCP Client:", sockAddr)
		conn, err = grpc.Dial(
			fmt.Sprintf("%s:///%s", resolver2.CustScheme, resolver2.CustServiceName),
			// This sets the initial load balancing policy as Round Robin
			grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.maxMessageSize), grpc.MaxCallSendMsgSize(opts.maxMessageSize)),
		)
	} else {
		sockAddr = fmt.Sprintf("%s:%s", shared.UDS, opts.udsSockAddr)
		log.Println("UDS Client:", sockAddr)
		conn, err = grpc.Dial(sockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.maxMessageSize), grpc.MaxCallSendMsgSize(opts.maxMessageSize)))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}
	c.conn = conn
	c.grpcClt = reducepb.NewReduceClient(conn)
	return c, nil
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
	var finalResponse *reducepb.ReduceResponse

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
			err = util.ToUDFErr("ReduceFn stream.Recv()", err)
			if err != nil {
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
