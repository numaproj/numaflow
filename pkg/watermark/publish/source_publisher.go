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

package publish

import (
	"context"
	"fmt"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// SourcePublisher publishes source watermarks based on a list of isb.ReadMessage
// and also publishes idle watermarks. It internally calls the main WM Publisher to publish
// the watermark. The only difference is that in source we do not care about offsets in the first
// Publish within the source itself (huh? :-D). Also, when the source boots up, it has to load
// the watermark information so it can know about the global source WM state.
type SourcePublisher interface {
	// PublishSourceWatermarks publishes source watermarks.
	PublishSourceWatermarks([]*isb.ReadMessage)
	// PublishIdleWatermarks publishes idle watermarks for the given partitions.
	PublishIdleWatermarks(wm time.Time, partitions []int32)
}

type sourcePublish struct {
	ctx                context.Context
	pipelineName       string
	vertexName         string
	srcPublishWMStores store.WatermarkStore
	sourcePublishWMs   map[int32]Publisher
	opts               *publishOptions
}

// NewSourcePublish returns a new source publisher.
func NewSourcePublish(ctx context.Context, pipelineName, vertexName string, srcPublishWMStores store.WatermarkStore, opts ...PublishOption) SourcePublisher {
	options := &publishOptions{
		defaultPartitionIdx: -1,
		delay:               0,
	}
	for _, opt := range opts {
		opt(options)
	}

	sp := &sourcePublish{
		ctx:                ctx,
		pipelineName:       pipelineName,
		vertexName:         vertexName,
		srcPublishWMStores: srcPublishWMStores,
		sourcePublishWMs:   make(map[int32]Publisher),
		opts:               options,
	}

	// if defaultPartitionIdx is set, create a publisher for it
	// will be used by http, nats and tickgen source whose partitionIdx is same
	// as the vertex replica index
	if sp.opts.defaultPartitionIdx != -1 {
		entityName := fmt.Sprintf("%s-%s-%d", sp.pipelineName, sp.vertexName, sp.opts.defaultPartitionIdx)
		processorEntity := entity.NewProcessorEntity(entityName)
		// toVertexPartitionCount is 1 because we publish watermarks within the source itself.
		sourcePublishWM := NewPublish(sp.ctx, processorEntity, sp.srcPublishWMStores, 1, IsSource(), WithDelay(sp.opts.delay))
		sp.sourcePublishWMs[sp.opts.defaultPartitionIdx] = sourcePublishWM
	}

	return sp
}

// PublishSourceWatermarks publishes source watermarks for a list of isb.ReadMessage.
// it publishes for the partitions to which the messages belong, it publishes the oldest timestamp
// seen for that partition in the list of messages.
// if a publisher for a partition doesn't exist, it creates one.
func (df *sourcePublish) PublishSourceWatermarks(readMessages []*isb.ReadMessage) {
	// oldestTimestamps stores the latest timestamps for different partitions
	oldestTimestamps := make(map[int32]time.Time)
	for _, m := range readMessages {
		// Get latest timestamps for different partitions
		if t, ok := oldestTimestamps[m.ReadOffset.PartitionIdx()]; !ok || m.EventTime.Before(t) {
			oldestTimestamps[m.ReadOffset.PartitionIdx()] = m.EventTime
		}
	}
	for p, t := range oldestTimestamps {
		publisher := df.loadSourceWatermarkPublisher(p)
		// toVertexPartitionIdx is 0 because we publish watermarks within the source itself.
		publisher.PublishWatermark(wmb.Watermark(t), nil, 0) // we don't care about the offset while publishing source watermark
	}
}

// PublishIdleWatermarks publishes idle watermarks for all partitions.
func (df *sourcePublish) PublishIdleWatermarks(wm time.Time, partitions []int32) {
	for _, partitionId := range partitions {
		publisher := df.loadSourceWatermarkPublisher(partitionId)
		publisher.PublishIdleWatermark(wmb.Watermark(wm), nil, 0) // while publishing idle watermark at source, we don't care about the offset
	}
}

// loadSourceWatermarkPublisher does a lazy load on the watermark publisher
func (df *sourcePublish) loadSourceWatermarkPublisher(partitionID int32) Publisher {
	if p, ok := df.sourcePublishWMs[partitionID]; ok {
		return p
	}
	entityName := fmt.Sprintf("%s-%s-%d", df.pipelineName, df.vertexName, partitionID)
	processorEntity := entity.NewProcessorEntity(entityName)
	// toVertexPartitionCount is 1 because we publish watermarks within the source itself.
	sourcePublishWM := NewPublish(df.ctx, processorEntity, df.srcPublishWMStores, 1, IsSource(), WithDelay(df.opts.delay))
	df.sourcePublishWMs[partitionID] = sourcePublishWM
	return sourcePublishWM
}
