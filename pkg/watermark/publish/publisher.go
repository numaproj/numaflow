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
	ctx             context.Context
	entity          processor.ProcessorEntitier
	keyspace        string
	heartbeatBucket nats.KeyValue
	js              nats.JetStreamContext
	log             *zap.SugaredLogger
	otBucket        nats.KeyValue
	headWatermark   processor.Watermark

	// opts
	// autoRefreshHeartbeat is not required for all processors. e.g. Kafka source doesn't need it
	autoRefreshHeartbeat bool
	podHeartbeatRate     int64
	bucketConfigs        *nats.KeyValueConfig
}

// NewPublish returns `Publish`.
// TODO: remove keyspace from signature
func NewPublish(ctx context.Context, processorEntity processor.ProcessorEntitier, keyspace string, js nats.JetStreamContext, heartbeatBucket nats.KeyValue, inputOpts ...PublishOption) *Publish {
	log := logging.FromContext(ctx)

	opts := &publishOptions{
		autoRefreshHeartbeat: false,
		podHeartbeatRate:     5,
		// TODO: use config to build default values
		// TODO: move it to control plane. One OT bucket for all the processors in a vertex.
		bucketConfigs: &nats.KeyValueConfig{
			Bucket:       processorEntity.GetBucketName(),
			Description:  fmt.Sprintf("[%s][%s] offset timeline bucket", processorEntity.GetPublishKeyspace(), processorEntity.GetBucketName()),
			MaxValueSize: 0,
			History:      1,
			TTL:          0,
			MaxBytes:     0,
			Storage:      0,
			Replicas:     0,
			Placement:    nil,
		},
	}
	for _, opt := range inputOpts {
		opt(opts)
	}

	p := &Publish{
		ctx:                  ctx,
		entity:               processorEntity,
		keyspace:             keyspace,
		heartbeatBucket:      heartbeatBucket,
		js:                   js,
		log:                  log,
		otBucket:             nil,
		autoRefreshHeartbeat: opts.autoRefreshHeartbeat,
		bucketConfigs:        opts.bucketConfigs,
		podHeartbeatRate:     opts.podHeartbeatRate,
	}

	p.initialSetup()

	if p.autoRefreshHeartbeat {
		go p.publishHeartbeat()
	}

	return p
}

// initialSetup creates the Offset-Timeline bucket and also inserts the default value
// as the ProcessorEntity starts emitting watermarks.
func (p *Publish) initialSetup() {
	// is this frowned upon?
	p.otBucket = p.createOTBucket(p.bucketConfigs)
	p.headWatermark = p.loadLatestFromStore()

	// TODO: put in the more initial values?
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
		_, err := p.otBucket.Put(key, value)
		if err != nil {
			p.log.Errorw("unable to publish watermark", zap.String("entity", p.entity.GetBucketName()), zap.String("key", key), zap.Error(err))
			// TODO: better exponential backoff
			time.Sleep(time.Millisecond * 250)
		} else {
			break
		}
	}
}

func (p *Publish) loadLatestFromStore() processor.Watermark {
	var otBucketName = p.entity.GetBucketName()
	var watermarks = p.getAllKeysFromBucket(otBucketName)
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
			_, err := p.heartbeatBucket.Put(p.entity.GetID(), []byte(fmt.Sprintf("%d", time.Now().Unix())))
			if err != nil {
				p.log.Errorw("put to bucket failed", zap.String("bucket", p.heartbeatBucket.Bucket()), zap.Error(err))
			}
		}
	}
}

// StopPublisher stops the publisher and cleans up the data associated with key.
func (p *Publish) StopPublisher() {
	// TODO: cleanup after processor dies
	//   - delete the Offset-Timeline bucket
	//   - remove itself from heartbeat bucket

	// TODO: move this to control plane and as of today control plane supports only shared OT buckets
	if !p.entity.IsSharedBucket() {
		// clean up offsetTimeline bucket
		bucketName := p.entity.GetBucketName()
		err := p.js.DeleteKeyValue(bucketName)
		if err != nil {
			p.log.Errorw("failed to delete bucket (nats.KeyValue)", zap.String("bucket", bucketName), zap.Error(err))
		}
	}

	// clean up heartbeat bucket
	err := p.heartbeatBucket.Delete(p.entity.GetID())
	if err != nil {
		p.log.Errorw("failed to delete the key in the heartbeat bucket", zap.String("bucket", p.keyspace), zap.String("key", p.entity.GetID()), zap.Error(err))
	}
}

func (p *Publish) createOTBucket(configs *nats.KeyValueConfig) nats.KeyValue {
	bucketName := p.entity.GetBucketName()
	// TODO: we don't delete any bucket
	kv, err := p.js.CreateKeyValue(configs)
	if err != nil {
		p.log.Fatalw("failed to create the bucket", zap.String("bucket", bucketName), zap.Error(err))
	}

	return kv
}

func (p *Publish) getAllKeysFromBucket(bucketName string) []string {
	kv, err := p.js.KeyValue(bucketName)
	if err != nil {
		p.log.Fatalw("failed to create the bucket", zap.String("bucket", bucketName), zap.Error(err))
	}
	keys, err := kv.Keys()
	if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
		p.log.Fatalw("failed to get the keys", zap.String("bucket", bucketName), zap.Error(err))
	}
	return keys
}
