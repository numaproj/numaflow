package client

import (
	"context"
	"time"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Datum contains methods to get the payload information.
type Datum interface {
	Value() []byte
	EventTime() time.Time
	Watermark() time.Time
	Metadata() DatumMetadata
}

// DatumMetadata contains methods to get the metadata information for datum.
type DatumMetadata interface {
	// ID returns the ID of the datum.
	ID() string
	// NumDelivered returns the number of times the datum has been delivered.
	NumDelivered() uint64
}

// Metadata contains methods to get the metadata for the reduce operation.
type Metadata interface {
	IntervalWindow() IntervalWindow
}

// IntervalWindow contains methods to get the information for a given interval window.
type IntervalWindow interface {
	StartTime() time.Time
	EndTime() time.Time
}

// Client contains methods to call a gRPC client.
type Client interface {
	CloseConn(ctx context.Context) error
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	MapFn(ctx context.Context, datum *functionpb.DatumRequest) ([]*functionpb.DatumResponse, error)
	MapStreamFn(ctx context.Context, datum *functionpb.DatumRequest, datumCh chan<- *functionpb.DatumResponse) error
	MapTFn(ctx context.Context, datum *functionpb.DatumRequest) ([]*functionpb.DatumResponse, error)
	ReduceFn(ctx context.Context, datumStreamCh <-chan *functionpb.DatumRequest) ([]*functionpb.DatumResponse, error)
}
