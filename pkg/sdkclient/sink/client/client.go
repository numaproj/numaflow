package client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/numaproj/numaflow-go/pkg/sink"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"github.com/numaproj/numaflow-go/pkg/info"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// client contains the grpc connection and the grpc client.
type client struct {
	conn    *grpc.ClientConn
	grpcClt sinkpb.UserDefinedSinkClient
}

var _ Client = (*client)(nil)

// New creates a new client object.
func New(inputOptions ...Option) (*client, error) {
	var opts = &options{
		sockAddr:                   sink.Addr,
		serverInfoFilePath:         info.ServerInfoFilePath,
		serverInfoReadinessTimeout: 120 * time.Second, // Default timeout is 120 seconds
		maxMessageSize:             1024 * 1024 * 64,  // 64 MB
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
	sockAddr := fmt.Sprintf("%s:%s", sink.Protocol, opts.sockAddr)
	conn, err := grpc.Dial(sockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.maxMessageSize), grpc.MaxCallSendMsgSize(opts.maxMessageSize)))
	if err != nil {
		return nil, fmt.Errorf("failed to execute grpc.Dial(%q): %w", sockAddr, err)
	}
	c.conn = conn
	c.grpcClt = sinkpb.NewUserDefinedSinkClient(conn)
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

// SinkFn applies a function to a list of datum elements.
func (c *client) SinkFn(ctx context.Context, datumList []*sinkpb.DatumRequest) ([]*sinkpb.Response, error) {
	stream, err := c.grpcClt.SinkFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute c.grpcClt.SinkFn(): %w", err)
	}
	for _, datum := range datumList {
		if err := stream.Send(datum); err != nil {
			return nil, fmt.Errorf("failed to execute stream.Send(%v): %w", datum, err)
		}
	}
	responseList, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("failed to execute stream.CloseAndRecv(): %w", err)
	}

	return responseList.GetResponses(), nil
}
