package client

import (
	"context"
	"time"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Datum interface {
	Keys() []string
	Value() []byte
	EventTime() time.Time
	Watermark() time.Time
	ID() string
}

// Client contains methods to call a gRPC client.
type Client interface {
	CloseConn(ctx context.Context) error
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	SinkFn(ctx context.Context, datumList []*sinkpb.DatumRequest) ([]*sinkpb.Response, error)
}
