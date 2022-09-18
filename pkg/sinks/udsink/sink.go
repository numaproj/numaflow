package udsink

import (
	"context"
	"fmt"
	"time"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type userDefinedSink struct {
	name         string
	pipelineName string
	isdf         *forward.InterStepDataForward
	logger       *zap.SugaredLogger
	udsink       *udsGRPCBasedUDSink
}

type Option func(*userDefinedSink) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(t *userDefinedSink) error {
		t.logger = log
		return nil
	}
}

// NewUserDefinedSink returns genericSink type.
func NewUserDefinedSink(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, opts ...Option) (*userDefinedSink, error) {
	s := new(userDefinedSink)
	name := vertex.Spec.Name
	s.name = name
	s.pipelineName = vertex.Spec.PipelineName
	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, err
		}
	}
	if s.logger == nil {
		s.logger = logging.NewLogger()
	}

	forwardOpts := []forward.Option{forward.WithLogger(s.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	udsink, err := NewUDSGRPCBasedUDSink()
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client, %w", err)
	}
	s.udsink = udsink
	isdf, err := forward.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{vertex.GetToBuffers()[0].Name: s}, forward.All, applier.Terminal, fetchWatermark, publishWatermark, forwardOpts...)
	if err != nil {
		return nil, err
	}
	s.isdf = isdf
	return s, nil
}

func (s *userDefinedSink) GetName() string {
	return s.name
}

func (s *userDefinedSink) IsFull() bool {
	return false
}

// Write writes to the UDSink container.
func (s *userDefinedSink) Write(ctx context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	msgs := make([]*sinkpb.Datum, len(messages))
	for i, m := range messages {
		msgs[i] = &sinkpb.Datum{
			// NOTE: key is not used anywhere ATM
			Id:        m.ID,
			Value:     m.Payload,
			EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(m.EventTime)},
			Watermark: &sinkpb.Watermark{Watermark: timestamppb.New(time.Time{})}, // TODO: insert the correct watermark
		}
	}
	return nil, s.udsink.Apply(ctx, msgs)
}

func (br *userDefinedSink) Close() error {
	// TODO: context.Background()
	return br.udsink.CloseConn(context.Background())
}

func (s *userDefinedSink) Start() <-chan struct{} {
	// Readiness check
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := s.udsink.WaitUntilReady(ctx); err != nil {
		s.logger.Fatalf("failed on UDSink readiness check, %w", err)
	}
	return s.isdf.Start()
}

func (s *userDefinedSink) Stop() {
	s.isdf.Stop()
}

func (s *userDefinedSink) ForceStop() {
	s.isdf.ForceStop()
}
