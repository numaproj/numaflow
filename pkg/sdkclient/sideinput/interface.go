package sideinput

import (
	"context"

	sideinputpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sideinput/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client contains methods to call a gRPC client for side input.
type Client interface {
	CloseConn(ctx context.Context) error
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	RetrieveSideInput(ctx context.Context, in *emptypb.Empty) (*sideinputpb.SideInputResponse, error)
	WaitUntilReady(ctx context.Context) error
	IsHealthy(ctx context.Context) error
}
