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

// package publish contains the logic to publish watermark. It exposes the `Publisher` interface.
// which has the methods to publish watermark. It also publishes the heartbeat for the processor entity.
// The heartbeat will be used to detect the processor is alive or not. It also has the method to
// publish Idle watermark if the processor is idle.

package publish

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// Publisher interface defines how to publish Watermark for a ProcessorEntitier.
type Publisher interface {
	io.Closer
	// PublishWatermark publishes the watermark.
	PublishWatermark(w wmb.Watermark, o isb.Offset, toVertexPartitionIdx int32)
	// PublishIdleWatermark publishes the idle watermark.
	PublishIdleWatermark(wm wmb.Watermark, o isb.Offset, toVertexPartitionIdx int32)
	// GetLatestWatermark returns the latest published watermark.
	GetLatestWatermark() wmb.Watermark
}

// publish publishes the watermark for a processor entity.
type publish struct {
	ctx    context.Context
	entity processor.ProcessorEntitier
	// heartbeatStore uses second as the time unit for the value
	heartbeatStore store.WatermarkKVStorer
	// osStore uses millisecond as the time unit for the value
	otStore                store.WatermarkKVStorer
	log                    *zap.SugaredLogger
	headWatermarks         []wmb.Watermark
	headWMLock             sync.RWMutex
	toVertexPartitionCount int32
	opts                   *publishOptions
}

// NewPublish returns `Publish`.
func NewPublish(ctx context.Context, processorEntity processor.ProcessorEntitier, watermarkStores store.WatermarkStorer, toVertexPartitionCount int32, inputOpts ...PublishOption) Publisher {
	log := logging.FromContext(ctx).With("entityID", processorEntity.GetName()).
		With("otStore", watermarkStores.OffsetTimelineStore().GetStoreName()).
		With("hbStore", watermarkStores.HeartbeatStore().GetStoreName())
	log.Info("Creating a new watermark publisher")
	opts := &publishOptions{
		autoRefreshHeartbeat: true,
		podHeartbeatRate:     5,
		isSource:             false,
		delay:                0,
	}
	for _, opt := range inputOpts {
		opt(opts)
	}

	p := &publish{
		ctx:                    ctx,
		entity:                 processorEntity,
		heartbeatStore:         watermarkStores.HeartbeatStore(),
		otStore:                watermarkStores.OffsetTimelineStore(),
		toVertexPartitionCount: toVertexPartitionCount,
		log:                    log,
		opts:                   opts,
	}

	p.initialSetup()

	if opts.autoRefreshHeartbeat {
		go p.publishHeartbeat()
	}
	return p
}

// GetHeadWM gets the headWatermark for the given partition.
func (p *publish) GetHeadWM(toVertexPartitionIdx int32) wmb.Watermark {
	p.headWMLock.RLock()
	defer p.headWMLock.RUnlock()
	return p.headWatermarks[toVertexPartitionIdx]
}

// SetHeadWM sets the headWatermark using the given wm for the given partition.
func (p *publish) SetHeadWM(wm wmb.Watermark, toVertexPartitionIdx int32) {
	p.headWMLock.Lock()
	defer p.headWMLock.Unlock()
	p.headWatermarks[toVertexPartitionIdx] = wm
}

// initialSetup inserts the default values as the ProcessorEntitier starts emitting watermarks.
// We will be initializing all to -1
// TODO: we could ideally resume from where we left off, but this will introduce a new key.
func (p *publish) initialSetup() {
	var headWms []wmb.Watermark
	for i := 0; i < int(p.toVertexPartitionCount); i++ {
		headWms = append(headWms, wmb.InitialWatermark)
	}
	p.headWatermarks = headWms
}

// PublishWatermark publishes watermark and will retry until it can succeed. It will not publish if the new-watermark
// is less than the current head watermark.
func (p *publish) PublishWatermark(wm wmb.Watermark, offset isb.Offset, toVertexPartitionIdx int32) {
	validWM, skipWM := p.validateWatermark(wm, toVertexPartitionIdx)
	if skipWM {
		return
	}

	var key = p.entity.GetName()

	// build value
	var seq int64
	if p.opts.isSource || p.opts.isSink {
		// For source and sink publisher, we don't care about the offset, also the sequence of the offset might not be integer.
		seq = time.Now().UnixNano()
	} else {
		seq, _ = offset.Sequence()
	}
	var otValue = wmb.WMB{
		Offset:    seq,
		Watermark: validWM.UnixMilli(),
		Partition: toVertexPartitionIdx,
	}

	value, err := otValue.EncodeToBytes()
	if err != nil {
		p.log.Errorw("Unable to publish watermark", zap.String("HB", p.heartbeatStore.GetStoreName()), zap.String("OT", p.otStore.GetStoreName()), zap.String("key", key), zap.Error(err))
	}

	for {
		err := p.otStore.PutKV(p.ctx, key, value)
		if err != nil {
			p.log.Errorw("Unable to publish watermark", zap.String("HB", p.heartbeatStore.GetStoreName()), zap.String("OT", p.otStore.GetStoreName()), zap.String("key", key), zap.Error(err))
			// TODO: better exponential backoff
			time.Sleep(time.Millisecond * 250)
		} else {
			p.log.Debugw("New watermark published with offset", zap.Int64("head", p.GetHeadWM(toVertexPartitionIdx).UnixMilli()), zap.Int64("new", validWM.UnixMilli()), zap.Int64("offset", seq))
			break
		}
	}
}

// validateWatermark checks if the new watermark is greater than the head watermark, return true if yes,
// otherwise, return false
func (p *publish) validateWatermark(wm wmb.Watermark, toVertexPartitionIdx int32) (wmb.Watermark, bool) {
	if p.opts.isSource && p.opts.delay.Nanoseconds() > 0 && !time.Time(wm).IsZero() {
		wm = wmb.Watermark(time.Time(wm).Add(-p.opts.delay))
	}
	// update p.headWatermarks only if wm > p.headWatermarks
	headWM := p.GetHeadWM(toVertexPartitionIdx)
	if wm.After(time.Time(headWM)) {
		p.log.Debugw("New watermark is updated for the head watermark", zap.String("head", headWM.String()), zap.String("new", wm.String()))
		p.SetHeadWM(wm, toVertexPartitionIdx)
	} else if wm.Before(time.Time(headWM)) {
		p.log.Warnw("Skip publishing the new watermark because it's older than the current watermark", zap.String("entity", p.entity.GetName()), zap.Int64("head", headWM.UnixMilli()), zap.Int64("new", wm.UnixMilli()))
		return wmb.Watermark{}, true
	} else {
		p.log.Debugw("Skip publishing the new watermark because it's the same as the current watermark", zap.String("entity", p.entity.GetName()), zap.Int64("head", headWM.UnixMilli()), zap.Int64("new", wm.UnixMilli()))
		return wmb.Watermark{}, true
	}
	return wm, false
}

// PublishIdleWatermark publishes the idle watermark and will retry until it can succeed.
// TODO: merge with PublishWatermark
func (p *publish) PublishIdleWatermark(wm wmb.Watermark, offset isb.Offset, toVertexPartitionIdx int32) {
	var key = p.entity.GetName()
	validWM, skipWM := p.validateWatermark(wm, toVertexPartitionIdx)
	if skipWM {
		return
	}
	// build value
	var seq int64
	if p.opts.isSource || p.opts.isSink {
		// for source and sink publisher, we don't care about the offset, also the sequence of the offset might not be integer.
		seq = time.Now().UnixNano()
	} else {
		seq, _ = offset.Sequence()
	}
	var otValue = wmb.WMB{
		Offset:    seq,
		Watermark: validWM.UnixMilli(),
		Idle:      true,
		Partition: toVertexPartitionIdx,
	}

	value, err := otValue.EncodeToBytes()
	if err != nil {
		p.log.Errorw("Unable to publish idle watermark", zap.String("HB", p.heartbeatStore.GetStoreName()), zap.String("OT", p.otStore.GetStoreName()), zap.String("key", key), zap.Error(err))
	}

	for {
		err := p.otStore.PutKV(p.ctx, key, value)
		if err != nil {
			p.log.Errorw("Unable to publish idle watermark", zap.String("HB", p.heartbeatStore.GetStoreName()), zap.String("OT", p.otStore.GetStoreName()), zap.String("key", key), zap.Error(err))
			// TODO: better exponential backoff
			time.Sleep(time.Millisecond * 250)
		} else {
			p.log.Debugw("New idle watermark published", zap.String("HB", p.heartbeatStore.GetStoreName()), zap.String("OT", p.otStore.GetStoreName()), zap.String("key", key), zap.Int64("offset", seq), zap.Int64("watermark", validWM.UnixMilli()))
			break
		}
	}
}

// loadLatestFromStore loads the latest watermark stored in the watermark store.
// TODO: how to repopulate if the processing unit is down for a really long time?
func (p *publish) loadLatestFromStore() wmb.Watermark {
	var (
		timeWatermark = wmb.InitialWatermark
		key           = p.entity.GetName()
	)
	byteValue, err := p.otStore.GetValue(p.ctx, key)
	// could happen during boot up
	if err != nil {
		p.log.Warnw("Unable to load latest watermark from wmb store (failed to get value from wmb store)", zap.String("OT", p.otStore.GetStoreName()), zap.String("processorEntity", p.entity.GetName()), zap.Error(err))
		return timeWatermark
	}
	otValue, err := wmb.DecodeToWMB(byteValue)
	if err != nil {
		p.log.Errorw("Unable to load latest watermark from wmb store (failed to decode wmb value)", zap.String("OT", p.otStore.GetStoreName()), zap.String("processorEntity", p.entity.GetName()), zap.Error(err))
		return timeWatermark
	}
	timeWatermark = wmb.Watermark(time.UnixMilli(otValue.Watermark))
	return timeWatermark
}

// GetLatestWatermark returns the latest watermark for that processor.
func (p *publish) GetLatestWatermark() wmb.Watermark {
	var latestWatermark = wmb.InitialWatermark
	for _, wm := range p.headWatermarks {
		if wm.After(time.Time(latestWatermark)) {
			latestWatermark = wm
		}
	}
	return latestWatermark
}

func (p *publish) publishHeartbeat() {
	ticker := time.NewTicker(time.Second * time.Duration(p.opts.podHeartbeatRate))
	defer ticker.Stop()
	p.log.Infow("Publishing Heartbeat ticker started")
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			err := p.heartbeatStore.PutKV(p.ctx, p.entity.GetName(), []byte(fmt.Sprintf("%d", time.Now().Unix())))
			if err != nil {
				p.log.Errorw("Put to bucket failed", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.Error(err))
			}
		}
	}
}

// Close stops the publisher and cleans up the data associated with key.
func (p *publish) Close() error {
	p.log.Info("Closing watermark publisher")
	defer func() {
		if p.otStore != nil {
			p.otStore.Close()
		}
		if p.heartbeatStore != nil {
			p.heartbeatStore.Close()
		}
	}()
	// TODO: cleanup after processor dies
	//   - delete the Offset-Timeline bucket
	//   - remove itself from heartbeat bucket

	// clean up heartbeat bucket
	if err := p.heartbeatStore.DeleteKey(p.ctx, p.entity.GetName()); err != nil {
		p.log.Errorw("Failed to delete the key in the heartbeat bucket", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.String("key", p.entity.GetName()), zap.Error(err))
		return err
	}
	return nil
}
