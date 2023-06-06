package client

import (
	"context"
	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client contains methods to call a gRPC client.
type Client interface {
	CloseConn(ctx context.Context) error
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	SinkFn(ctx context.Context, datumList []*sinkpb.DatumRequest) ([]*sinkpb.Response, error)
}
