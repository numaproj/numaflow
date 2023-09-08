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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sourceforward "github.com/numaproj/numaflow/pkg/sources/forward"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
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
	// name of the user-defined source vertex
	vertexName string
	// name of the pipeline
	pipelineName string
	// sourceApplier applies the user-defined source functions
	sourceApplier *GRPCBasedUDSource
	// forwarder writes the source data to destination
	forwarder *sourceforward.DataForward
	// context cancel function
	cancelFn context.CancelFunc
	// source watermark publishers for different partitions
	srcWMPublishers map[int32]publish.Publisher
	// source watermark publisher stores
	srcPublishWMStores store.WatermarkStore
	// lifecycleCtx context is used to control the lifecycle of this source.
	lifecycleCtx context.Context
	// read timeout for the source
	readTimeout time.Duration
	// logger
	logger *zap.SugaredLogger
	// a lock to protect srcWMPublishers
	lock *sync.RWMutex
}

func New(
	vertexInstance *dfv1.VertexInstance,
	writers map[string][]isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	transformer applier.SourceTransformApplier,
	sourceApplier *GRPCBasedUDSource,
	fetchWM fetch.Fetcher,
	toVertexPublisherStores map[string]store.WatermarkStore,
	publishWMStores store.WatermarkStore,
	idleManager wmb.IdleManagement,
	opts ...Option) (*userDefinedSource, error) {

	u := &userDefinedSource{
		vertexName:         vertexInstance.Vertex.Spec.Name,
		pipelineName:       vertexInstance.Vertex.Spec.PipelineName,
		sourceApplier:      sourceApplier,
		srcPublishWMStores: publishWMStores,
		srcWMPublishers:    make(map[int32]publish.Publisher),
		lock:               new(sync.RWMutex),
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
	var err error
	u.forwarder, err = sourceforward.NewDataForward(vertexInstance.Vertex, u, writers, fsd, transformer, fetchWM, u, toVertexPublisherStores, idleManager, forwardOpts...)
	if err != nil {
		u.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	u.cancelFn = cancel
	u.lifecycleCtx = ctx
	return u, nil
}

// GetName returns the name of the user-defined source vertex
func (u *userDefinedSource) GetName() string {
	return u.vertexName
}

// GetPartitionIdx returns the partition number for the user-defined source.
// Source is like a buffer with only one partition. So, we always return 0
func (u *userDefinedSource) GetPartitionIdx() int32 {
	return 0
}

// Read reads the messages from the user-defined source
func (u *userDefinedSource) Read(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
	return u.sourceApplier.ApplyReadFn(ctx, count, u.readTimeout)
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

func (u *userDefinedSource) NoAck(_ context.Context, _ []isb.Offset) {
	panic("User defined source does not support NoAck")
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

func (u *userDefinedSource) PublishSourceWatermarks(msgs []*isb.ReadMessage) {
	// oldestTimestamps stores the latest timestamps for different partitions
	oldestTimestamps := make(map[int32]time.Time)
	for _, m := range msgs {
		// Get latest timestamps for different partitions
		if t, ok := oldestTimestamps[m.ReadOffset.PartitionIdx()]; !ok || m.EventTime.Before(t) {
			oldestTimestamps[m.ReadOffset.PartitionIdx()] = m.EventTime
		}
	}
	for p, t := range oldestTimestamps {
		publisher := u.loadSourceWatermarkPublisher(p)
		// toVertexPartitionIdx is 0 because we publish watermarks within the source itself.
		publisher.PublishWatermark(wmb.Watermark(t), nil, 0) // Source publisher does not care about the offset
	}
}

// loadSourceWatermarkPublisher does a lazy load on the watermark publisher
func (u *userDefinedSource) loadSourceWatermarkPublisher(partitionID int32) publish.Publisher {
	u.lock.Lock()
	defer u.lock.Unlock()
	if p, ok := u.srcWMPublishers[partitionID]; ok {
		return p
	}
	entityName := fmt.Sprintf("%s-%s-%d", u.pipelineName, u.vertexName, partitionID)
	processorEntity := entity.NewProcessorEntity(entityName)
	// toVertexPartitionCount is 1 because we publish watermarks within the source itself.
	sourcePublishWM := publish.NewPublish(u.lifecycleCtx, processorEntity, u.srcPublishWMStores, 1, publish.IsSource())
	u.srcWMPublishers[partitionID] = sourcePublishWM
	return sourcePublishWM
}
