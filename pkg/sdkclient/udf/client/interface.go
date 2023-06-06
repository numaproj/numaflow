package client

import (
	"context"
	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client contains methods to call a gRPC client.
type Client interface {
	CloseConn(ctx context.Context) error
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	MapFn(ctx context.Context, datum *functionpb.DatumRequest) ([]*functionpb.DatumResponse, error)
	MapStreamFn(ctx context.Context, datum *functionpb.DatumRequest, datumCh chan<- *functionpb.DatumResponse) error
	MapTFn(ctx context.Context, datum *functionpb.DatumRequest) ([]*functionpb.DatumResponse, error)
	ReduceFn(ctx context.Context, datumStreamCh <-chan *functionpb.DatumRequest) ([]*functionpb.DatumResponse, error)
}
