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

package udsource

import (
	"context"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sourceforward "github.com/numaproj/numaflow/pkg/sources/forward"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type Option func(*userDefinedSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(u *userDefinedSource) error {
		u.logger = l
		return nil
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(t time.Duration) Option {
	return func(u *userDefinedSource) error {
		u.readTimeout = t
		return nil
	}
}

type userDefinedSource struct {
	vertexName         string                     // name of the user-defined source vertex
	pipelineName       string                     // name of the pipeline
	sourceApplier      *GRPCBasedUDSource         // sourceApplier applies the user-defined source functions
	forwarder          *sourceforward.DataForward // forwarder writes the source data to destination
	cancelFn           context.CancelFunc         // context cancel function
	srcPublishWMStores store.WatermarkStore       // source watermark publisher stores
	lifecycleCtx       context.Context            // lifecycleCtx context is used to control the lifecycle of this source.
	readTimeout        time.Duration              // read timeout for the source
	partitions         map[int32]struct{}         // partitions of the source
	logger             *zap.SugaredLogger
}

func New(
	vertexInstance *dfv1.VertexInstance,
	writers map[string][]isb.BufferWriter,
	fsd forwarder.ToWhichStepDecider,
	transformer applier.SourceTransformApplier,
	sourceApplier *GRPCBasedUDSource,
	fetchWM fetch.SourceFetcher,
	toVertexPublisherStores map[string]store.WatermarkStore,
	publishWMStores store.WatermarkStore,
	idleManager wmb.IdleManager,
	opts ...Option) (sourcer.Sourcer, error) {

	var err error

	u := &userDefinedSource{
		vertexName:         vertexInstance.Vertex.Spec.Name,
		pipelineName:       vertexInstance.Vertex.Spec.PipelineName,
		sourceApplier:      sourceApplier,
		srcPublishWMStores: publishWMStores,
		partitions:         make(map[int32]struct{}),
		logger:             logging.NewLogger(), // default logger
	}
	for _, opt := range opts {
		if err := opt(u); err != nil {
			return nil, err
		}
	}

	forwardOpts := []sourceforward.Option{sourceforward.WithLogger(u.logger)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sourceforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	u.cancelFn = cancel
	u.lifecycleCtx = ctx

	// create a source watermark publisher
	sourceWmPublisher := publish.NewSourcePublish(ctx, u.pipelineName, u.vertexName, publishWMStores, publish.WithDelay(vertexInstance.Vertex.Spec.Watermark.GetMaxDelay()))

	u.forwarder, err = sourceforward.NewDataForward(vertexInstance, u, writers, fsd, transformer, fetchWM, sourceWmPublisher, toVertexPublisherStores, idleManager, forwardOpts...)
	if err != nil {
		u.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	return u, nil
}

// GetName returns the name of the user-defined source vertex
func (u *userDefinedSource) GetName() string {
	return u.vertexName
}

// Partitions returns the partitions of the user-defined source
func (u *userDefinedSource) Partitions() []int32 {
	partitions := make([]int32, 0, len(u.partitions))
	for partition := range u.partitions {
		partitions = append(partitions, partition)
	}
	return partitions
}

// Read reads the messages from the user-defined source, tracks the partitions from which the messages are read
// tracked partitions are used to determine the partitions to which the watermarks should be published
func (u *userDefinedSource) Read(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
	readMessages, err := u.sourceApplier.ApplyReadFn(ctx, count, u.readTimeout)
	if err != nil {
		return nil, err
	}
	for _, msg := range readMessages {
		if _, ok := u.partitions[msg.ReadOffset.PartitionIdx()]; !ok {
			u.partitions[msg.ReadOffset.PartitionIdx()] = struct{}{}
		}
	}
	return readMessages, nil
}

// Ack acknowledges the messages from the user-defined source
// If there is an error, return the error using an error array
func (u *userDefinedSource) Ack(ctx context.Context, offsets []isb.Offset) []error {
	return []error{u.sourceApplier.ApplyAckFn(ctx, offsets)}
}

// Pending returns the number of pending messages in the user-defined source
func (u *userDefinedSource) Pending(ctx context.Context) (int64, error) {
	return u.sourceApplier.ApplyPendingFn(ctx)
}

func (u *userDefinedSource) Close() error {
	u.logger.Info("Shutting down user-defined source...")
	u.cancelFn()
	return u.sourceApplier.CloseConn(context.Background())
}

func (u *userDefinedSource) Stop() {
	u.forwarder.Stop()
	u.logger.Info("forwarder stopped successfully")
}

func (u *userDefinedSource) ForceStop() {
	u.forwarder.ForceStop()
	u.logger.Info("forwarder force stopped successfully")
}

// Start starts the data forwarding
func (u *userDefinedSource) Start() <-chan struct{} {
	u.logger.Info("Starting user-defined source...")
	return u.forwarder.Start()
}
