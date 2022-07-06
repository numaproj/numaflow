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

// Publish publishes the watermark for a processor entity.
type Publish struct {
	ctx            context.Context
	entity         processor.ProcessorEntitier
	heartbeatStore store.WatermarkKVStorer
	otStore        store.WatermarkKVStorer
	log            *zap.SugaredLogger
	headWatermark  processor.Watermark

	// opts
	// autoRefreshHeartbeat is not required for all processors. e.g. Kafka source doesn't need it
	autoRefreshHeartbeat bool
	podHeartbeatRate     int64
}

// NewPublish returns `Publish`.
func NewPublish(ctx context.Context, processorEntity processor.ProcessorEntitier, hbStore store.WatermarkKVStorer, otStore store.WatermarkKVStorer, inputOpts ...PublishOption) *Publish {

	log := logging.FromContext(ctx)

	opts := &publishOptions{
		autoRefreshHeartbeat: true,
		podHeartbeatRate:     5,
	}
	for _, opt := range inputOpts {
		opt(opts)
	}

	p := &Publish{
		ctx:                  ctx,
		entity:               processorEntity,
		heartbeatStore:       hbStore,
		otStore:              otStore,
		log:                  log,
		autoRefreshHeartbeat: opts.autoRefreshHeartbeat,
		podHeartbeatRate:     opts.podHeartbeatRate,
	}

	p.initialSetup()

	// TODO: i do not think we need this autoRefreshHeartbeat flag anymore. Remove it?
	if p.autoRefreshHeartbeat {
		go p.publishHeartbeat()
	}

	return p
}

// initialSetup inserts the default values as the ProcessorEntity starts emitting watermarks.
func (p *Publish) initialSetup() {
	p.headWatermark = p.loadLatestFromStore()
}

// PublishWatermark publishes watermark and will retry until it can succeed. It will not publish if the new-watermark
// is less than the current head watermark.
func (p *Publish) PublishWatermark(wm processor.Watermark, offset isb.Offset) {
	// update p.headWatermark only if wm > p.headWatermark
	if !time.Time(wm).Before(time.Time(p.headWatermark)) {
		p.headWatermark = wm
	} else {
		p.log.Errorw("new watermark is older than the current watermark", zap.String("head", p.headWatermark.String()), zap.String("new", wm.String()))
		return
	}

	// build key (we may or may not share the same OT bucket)
	var key = p.entity.BuildOTWatcherKey(wm)

	// build value (offset)
	value := make([]byte, 8)
	o, _ := offset.Sequence()
	binary.LittleEndian.PutUint64(value, uint64(o))

	for {
		err := p.otStore.PutKV(p.ctx, key, value)
		if err != nil {
			p.log.Errorw("unable to publish watermark", zap.String("entity", p.entity.GetBucketName()), zap.String("key", key), zap.Error(err))
			// TODO: better exponential backoff
			time.Sleep(time.Millisecond * 250)
		} else {
			break
		}
	}
}

// loadLatestFromStore loads the latest watermark stored in the watermark store.
// TODO: how to repopulate if the processing unit is down for a really long time?
func (p *Publish) loadLatestFromStore() processor.Watermark {
	var watermarks = p.getAllOTKeysFromBucket()
	var latestWatermark int64 = math.MinInt64

	// skip all entries that do not match the prefix if we are sharing bucket

	for _, wm := range watermarks {
		epoch, skip, err := p.entity.ParseOTWatcherKey(wm)
		if skip {
			continue
		}
		if err != nil {
			p.log.Panicw("invalid epoch time string", zap.Error(err))
		}
		if latestWatermark < epoch {
			latestWatermark = epoch
		}
	}
	var timeWatermark = time.Unix(latestWatermark, 0)
	return processor.Watermark(timeWatermark)
}

// GetLatestWatermark returns the latest watermark for that processor.
func (p *Publish) GetLatestWatermark() processor.Watermark {
	return p.headWatermark
}

func (p *Publish) publishHeartbeat() {
	ticker := time.NewTicker(time.Second * time.Duration(p.podHeartbeatRate))
	defer ticker.Stop()
	p.log.Infow("Refreshing ActiveProcessors ticker started")
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			err := p.heartbeatStore.PutKV(p.ctx, p.entity.GetID(), []byte(fmt.Sprintf("%d", time.Now().Unix())))
			if err != nil {
				p.log.Errorw("put to bucket failed", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.Error(err))
			}
		}
	}
}

// StopPublisher stops the publisher and cleans up the data associated with key.
func (p *Publish) StopPublisher() {
	// TODO: cleanup after processor dies
	//   - delete the Offset-Timeline bucket
	//   - remove itself from heartbeat bucket

	p.log.Infow("stopping publisher", zap.String("bucket", p.heartbeatStore.GetStoreName()))
	if !p.entity.IsOTBucketShared() {
		p.log.Errorw("non sharing of bucket is not supported by controller as of today", zap.String("bucket", p.heartbeatStore.GetStoreName()))
	}

	// clean up heartbeat bucket
	err := p.heartbeatStore.DeleteKey(p.ctx, p.entity.GetID())
	if err != nil {
		p.log.Errorw("failed to delete the key in the heartbeat bucket", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.String("key", p.entity.GetID()), zap.Error(err))
	}
}

func (p *Publish) getAllOTKeysFromBucket() []string {
	keys, err := p.otStore.GetAllKeys(p.ctx)
	if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
		p.log.Fatalw("failed to get the keys", zap.String("bucket", p.heartbeatStore.GetStoreName()), zap.Error(err))
	}
	return keys
}
