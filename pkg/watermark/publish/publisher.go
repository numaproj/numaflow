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
	"io"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/ot"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// Publisher interface defines how to publish Watermark for a ProcessorEntitier.
type Publisher interface {
	io.Closer
	// PublishWatermark publishes the watermark.
	PublishWatermark(processor.Watermark, isb.Offset)
	// GetLatestWatermark returns the latest published watermark.
	GetLatestWatermark() processor.Watermark
}

// publish publishes the watermark for a processor entity.
type publish struct {
	ctx    context.Context
	entity processor.ProcessorEntitier
	// heartbeatStore uses second as the time unit for the value
	heartbeatStore store.WatermarkKVStorer
	// osStore uses millisecond as the time unit for the value
	otStore       store.WatermarkKVStorer
	log           *zap.SugaredLogger
	headWatermark processor.Watermark
	opts          *publishOptions
}

// NewPublish returns `Publish`.
func NewPublish(ctx context.Context, processorEntity processor.ProcessorEntitier, watermarkStores store.WatermarkStorer, inputOpts ...PublishOption) Publisher {
	log := logging.FromContext(ctx).With("entityID", processorEntity.GetID()).
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
		ctx:            ctx,
		entity:         processorEntity,
		heartbeatStore: watermarkStores.HeartbeatStore(),
		otStore:        watermarkStores.OffsetTimelineStore(),
		log:            log,
		opts:           opts,
	}

	p.initialSetup()

	if opts.autoRefreshHeartbeat {
		go p.publishHeartbeat()
	}
	return p
}

// initialSetup inserts the default values as the ProcessorEntitier starts emitting watermarks.
func (p *publish) initialSetup() {
	p.headWatermark = p.loadLatestFromStore()
}

// PublishWatermark publishes watermark and will retry until it can succeed. It will not publish if the new-watermark
// is less than the current head watermark.
func (p *publish) PublishWatermark(wm processor.Watermark, offset isb.Offset) {
	if p.opts.isSource && p.opts.delay.Nanoseconds() > 0 && !time.Time(wm).IsZero() {
		wm = processor.Watermark(time.Time(wm).Add(-p.opts.delay))
	}
	// update p.headWatermark only if wm > p.headWatermark
	if wm.After(time.Time(p.headWatermark)) {
		p.log.Debugw("New watermark is updated for the head watermark", zap.String("head", p.headWatermark.String()), zap.String("new", wm.String()))
		p.headWatermark = wm
	} else if wm.Before(time.Time(p.headWatermark)) {
		p.log.Warnw("Skip publishing the new watermark because it's older than the current watermark", zap.String("head", p.headWatermark.String()), zap.String("new", wm.String()))
		return
	} else {
		p.log.Debugw("Skip publishing the new watermark because it's the same as the current watermark", zap.String("head", p.headWatermark.String()), zap.String("new", wm.String()))
		return
	}

	var key = p.entity.BuildOTWatcherKey()

	// build value
	var seq int64
	if p.opts.isSource || p.opts.isSink {
		// For source and sink publisher, we don't care about the offset, also the sequence of the offset might not be integer.
		seq = time.Now().UnixNano()
	} else {
		seq, _ = offset.Sequence()
	}
	var otValue = ot.Value{
		Offset:    seq,
		Watermark: wm.UnixMilli(),
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
			p.log.Debugw("New watermark published with offset", zap.Int64("head", p.headWatermark.UnixMilli()), zap.Int64("new", wm.UnixMilli()), zap.Int64("offset", seq))
			break
		}
	}
}

// loadLatestFromStore loads the latest watermark stored in the watermark store.
// TODO: how to repopulate if the processing unit is down for a really long time?
func (p *publish) loadLatestFromStore() processor.Watermark {
	var (
		timeWatermark = time.UnixMilli(-1)
		key           = p.entity.BuildOTWatcherKey()
	)
	byteValue, err := p.otStore.GetValue(p.ctx, key)
	if err != nil {
		p.log.Errorw("Unable to load latest watermark from ot store (failed to get value from ot store)", zap.String("OT", p.otStore.GetStoreName()), zap.String("processorEntity", p.entity.GetID()), zap.Error(err))
		return processor.Watermark(timeWatermark)
	}
	otValue, err := ot.DecodeToOTValue(byteValue)
	if err != nil {
		p.log.Errorw("Unable to load latest watermark from ot store (failed to decode ot value)", zap.String("OT", p.otStore.GetStoreName()), zap.String("processorEntity", p.entity.GetID()), zap.Error(err))
		return processor.Watermark(timeWatermark)
	}
	timeWatermark = time.UnixMilli(otValue.Watermark)
	return processor.Watermark(timeWatermark)
}

// GetLatestWatermark returns the latest watermark for that processor.
func (p *publish) GetLatestWatermark() processor.Watermark {
	return p.headWatermark
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
			err := p.heartbeatStore.PutKV(p.ctx, p.entity.GetID(), []byte(fmt.Sprintf("%d", time.Now().Unix())))
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
	if err := p.heartbeatStore.DeleteKey(p.ctx, p.entity.GetID()); err != nil {
		p.log.Errorw("Failed to delete the key in the heartbeat bucket", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.String("key", p.entity.GetID()), zap.Error(err))
		return err
	}
	return nil
}
