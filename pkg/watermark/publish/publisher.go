package publish

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"go.uber.org/zap"
)

// Publisher interface defines how to publish Watermark for a ProcessorEntity.
type Publisher interface {
	// PublishWatermark publishes the watermark.
	PublishWatermark(processor.Watermark, isb.Offset)
	// GetLatestWatermark returns the latest published watermark.
	GetLatestWatermark() processor.Watermark
	// StopPublisher stops the publisher
	StopPublisher()
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

	log := logging.FromContext(ctx)

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

// initialSetup inserts the default values as the ProcessorEntity starts emitting watermarks.
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

	// build key (we may or may not share the same OT bucket)
	var key = p.entity.BuildOTWatcherKey(wm)

	// build value (offset)
	value := make([]byte, 8)
	var seq int64
	if p.opts.isSource || p.opts.isSink {
		// For source and sink publisher, we don't care about the offset, also the sequence of the offset might not be integer.
		seq = time.Now().UnixNano()
	} else {
		seq, _ = offset.Sequence()
	}
	binary.LittleEndian.PutUint64(value, uint64(seq))

	for {
		err := p.otStore.PutKV(p.ctx, key, value)
		if err != nil {
			p.log.Errorw("Unable to publish watermark", zap.String("HB", p.heartbeatStore.GetStoreName()), zap.String("OT", p.otStore.GetStoreName()), zap.String("key", key), zap.Error(err))
			// TODO: better exponential backoff
			time.Sleep(time.Millisecond * 250)
		} else {
			break
		}
	}
}

// loadLatestFromStore loads the latest watermark stored in the watermark store.
// TODO: how to repopulate if the processing unit is down for a really long time?
func (p *publish) loadLatestFromStore() processor.Watermark {
	// TODO: this is too much.
	var watermarks = p.getAllOTKeysFromBucket()
	var latestWatermark int64 = math.MinInt64

	// skip all entries that do not match the prefix if we are sharing bucket

	for _, wm := range watermarks {
		epoch, skip, err := p.entity.ParseOTWatcherKey(wm)
		if skip {
			continue
		}
		if err != nil {
			p.log.Errorw("Invalid epoch time string", zap.Error(err))
			continue
		}
		if latestWatermark < epoch {
			latestWatermark = epoch
		}
	}
	var timeWatermark = time.UnixMilli(latestWatermark)
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

// StopPublisher stops the publisher and cleans up the data associated with key.
func (p *publish) StopPublisher() {
	// TODO: cleanup after processor dies
	//   - delete the Offset-Timeline bucket
	//   - remove itself from heartbeat bucket

	p.log.Infow("Stopping publisher", zap.String("bucket", p.heartbeatStore.GetStoreName()))
	if !p.entity.IsOTBucketShared() {
		p.log.Warnw("Non sharing of bucket is not supported by controller as of today", zap.String("bucket", p.heartbeatStore.GetStoreName()))
	}

	// clean up heartbeat bucket
	err := p.heartbeatStore.DeleteKey(p.ctx, p.entity.GetID())
	if err != nil {
		p.log.Errorw("Failed to delete the key in the heartbeat bucket", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.String("key", p.entity.GetID()), zap.Error(err))
	}

	p.otStore.Close()
	p.heartbeatStore.Close()
}

func (p *publish) getAllOTKeysFromBucket() []string {
	keys, err := p.otStore.GetAllKeys(p.ctx)
	if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
		p.log.Fatalw("Failed to get the keys", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.Error(err))
	}
	return keys
}
