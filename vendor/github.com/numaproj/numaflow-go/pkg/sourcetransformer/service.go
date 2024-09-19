package sourcetransformer

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1"
)

const (
	uds                   = "unix"
	defaultMaxMessageSize = 1024 * 1024 * 64
	address               = "/var/run/numaflow/sourcetransform.sock"
	serverInfoFilePath    = "/var/run/numaflow/sourcetransformer-server-info"
)

// Service implements the proto gen server interface and contains the transformer operation
// handler.
type Service struct {
	v1.UnimplementedSourceTransformServer
	Transformer SourceTransformer
	shutdownCh  chan<- struct{}
}

// IsReady returns true to indicate the gRPC connection is ready.
func (fs *Service) IsReady(context.Context, *emptypb.Empty) (*v1.ReadyResponse, error) {
	return &v1.ReadyResponse{Ready: true}, nil
}

// SourceTransformFn applies a function to each request element.
// In addition to map function, SourceTransformFn also supports assigning a new event time to response.
// SourceTransformFn can be used only at source vertex by source data transformer.
func (fs *Service) SourceTransformFn(ctx context.Context, d *v1.SourceTransformRequest) (*v1.SourceTransformResponse, error) {
	// handle panic
	defer func() {
		if r := recover(); r != nil {
			fs.shutdownCh <- struct{}{}
		}
	}()
	var hd = NewHandlerDatum(d.GetValue(), d.EventTime.AsTime(), d.Watermark.AsTime(), d.Headers)
	messageTs := fs.Transformer.Transform(ctx, d.GetKeys(), hd)
	var results []*v1.SourceTransformResponse_Result
	for _, m := range messageTs.Items() {
		results = append(results, &v1.SourceTransformResponse_Result{
			EventTime: timestamppb.New(m.EventTime()),
			Keys:      m.Keys(),
			Value:     m.Value(),
			Tags:      m.Tags(),
		})
	}
	responseList := &v1.SourceTransformResponse{
		Results: results,
	}
	return responseList, nil
}
