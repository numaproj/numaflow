/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package udsink

import (
	"context"
	"fmt"
	"sync"
	"time"

	sinkpb "github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v2"
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

type UserDefinedSink struct {
	name         string
	pipelineName string
	isdf         *forward.InterStepDataForward
	logger       *zap.SugaredLogger
	udsink       *udsGRPCBasedUDSink
}

type Option func(*UserDefinedSink) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(t *UserDefinedSink) error {
		t.logger = log
		return nil
	}
}

// NewUserDefinedSink returns genericSink type.
func NewUserDefinedSink(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, opts ...Option) (*UserDefinedSink, error) {
	s := new(UserDefinedSink)
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

	forwardOpts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeSink), forward.WithLogger(s.logger)}
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

func (s *UserDefinedSink) GetName() string {
	return s.name
}

func (s *UserDefinedSink) IsFull() bool {
	return false
}

// Write writes to the UDSink container.
func (s *UserDefinedSink) Write(ctx context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	var (
		datumCh      = make(chan *sinkpb.Datum)
		responseList []*sinkpb.Response
		err          error
		errs         []error
		wg           sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		// start listening on datumCh
		responseList, err = s.udsink.Apply(ctx, datumCh)
	}()

	// start sending data to datumCh
	for _, m := range messages {
		datumCh <- &sinkpb.Datum{
			// NOTE: key is not used anywhere ATM
			Id:        m.ID,
			Value:     m.Payload,
			EventTime: &sinkpb.EventTime{EventTime: timestamppb.New(m.EventTime)},
			// Watermark is only available in readmessage....
			Watermark: &sinkpb.Watermark{Watermark: timestamppb.New(time.Time{})}, // TODO: insert the correct watermark
		}
	}
	// complete sending data, close the datumCh
	close(datumCh)
	wg.Wait()

	// processing error list
	if err != nil {
		for i := range responseList {
			errs[i] = ApplyUDSinkErr{
				UserUDSinkErr: false,
				Message:       fmt.Sprintf("gRPC client.SinkFn failed, %s", err),
				InternalErr: InternalErr{
					Flag:        true,
					MainCarDown: false,
				},
			}
		}
		return nil, errs
	}
	// Use ID to map the response messages, so that there's no strict requirement for the user defined sink to return the responseList in order.
	resMap := make(map[string]*sinkpb.Response)
	for _, res := range responseList {
		resMap[res.GetId()] = res
	}
	for i, m := range messages {
		if r, existing := resMap[m.ID]; !existing {
			errs[i] = fmt.Errorf("not found in responseList")
		} else {
			if !r.Success {
				if r.GetErrMsg() != "" {
					errs[i] = fmt.Errorf(r.GetErrMsg())
				} else {
					errs[i] = fmt.Errorf("unsuccessful due to unknown reason")
				}
			}
		}
	}
	return nil, errs
}

func (br *UserDefinedSink) Close() error {
	if br.udsink != nil {
		return br.udsink.CloseConn(context.Background())
	}
	return nil
}

func (s *UserDefinedSink) Start() <-chan struct{} {
	// Readiness check
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := s.udsink.WaitUntilReady(ctx); err != nil {
		s.logger.Fatalf("failed on UDSink readiness check, %w", err)
	}
	return s.isdf.Start()
}

func (s *UserDefinedSink) Stop() {
	s.isdf.Stop()
}

func (s *UserDefinedSink) ForceStop() {
	s.isdf.ForceStop()
}

// IsHealthy checks if the udsink sidecar is healthy.
func (s *UserDefinedSink) IsHealthy() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return s.udsink.WaitUntilReady(ctx)
}
