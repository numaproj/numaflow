package logger

import (
	"context"
	"log"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/applier"
)

// ToLog prints the output to a log sinks.
type ToLog struct {
	name         string
	pipelineName string
	isdf         *forward.InterStepDataForward
	logger       *zap.SugaredLogger
}

type Option func(*ToLog) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(t *ToLog) error {
		t.logger = log
		return nil
	}
}

// NewToLog returns ToLog type.
func NewToLog(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, opts ...Option) (*ToLog, error) {
	toLog := new(ToLog)
	name := vertex.Spec.Name
	toLog.name = name
	toLog.pipelineName = vertex.Spec.PipelineName
	// use opts in future for specifying logger format etc
	for _, o := range opts {
		if err := o(toLog); err != nil {
			return nil, err
		}
	}
	if toLog.logger == nil {
		toLog.logger = logging.NewLogger()
	}

	forwardOpts := []forward.Option{forward.WithLogger(toLog.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	isdf, err := forward.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{name: toLog}, forward.All, applier.Terminal, nil, nil, forwardOpts...)
	if err != nil {
		return nil, err
	}
	toLog.isdf = isdf

	return toLog, nil
}

// GetName returns the name.
func (s *ToLog) GetName() string {
	return s.name
}

// IsFull returns whether logging is full, which is never true.
func (s *ToLog) IsFull() bool {
	// printing can never be full
	return false
}

// Write writes to the log.
func (s *ToLog) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	prefix := "(" + s.GetName() + ")"
	for _, message := range messages {
		logSinkWriteCount.With(map[string]string{metricspkg.LabelVertex: s.name, metricspkg.LabelPipeline: s.pipelineName}).Inc()
		log.Println(prefix, string(message.Payload))
	}
	return nil, make([]error, len(messages))
}

func (br *ToLog) Close() error {
	return nil
}

// Start starts sinking to Log.
func (s *ToLog) Start() <-chan struct{} {
	return s.isdf.Start()
}

// Stop stops sinking
func (s *ToLog) Stop() {
	s.isdf.Stop()
}

// ForceStop stops sinking
func (s *ToLog) ForceStop() {
	s.isdf.ForceStop()
}
