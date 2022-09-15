package udsink

import (
	"context"
	"time"

	sinksdk "github.com/numaproj/numaflow-go/sink"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"go.uber.org/zap"
)

type userDefinedSink struct {
	name         string
	pipelineName string
	isdf         *forward.InterStepDataForward
	logger       *zap.SugaredLogger
	udsink       *udsHTTPBasedUDSink
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
	contentType := sharedutil.LookupEnvStringOr(dfv1.EnvUDSinkContentType, string(dfv1.MsgPackType))
	s.udsink = NewUDSHTTPBasedUDSink(dfv1.PathVarRun+"/udsink.sock", withTimeout(20*time.Second), withContentType(dfv1.ContentType(contentType)))
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
	msgs := make([]sinksdk.Message, len(messages))
	for i, m := range messages {
		msgs[i] = sinksdk.Message{ID: m.ID, Payload: m.Payload}
	}
	return nil, s.udsink.Apply(ctx, msgs)
}

func (br *userDefinedSink) Close() error {
	return nil
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
