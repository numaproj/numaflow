package fetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/numaproj/numaflow/pkg/watermark/processor"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"

	"github.com/nats-io/nats.go"
)

type status int

const (
	_active status = iota
	_inactive
	_deleted
)

func (s status) String() string {
	switch s {
	case _active:
		return "active"
	case _inactive:
		return "inactive"
	case _deleted:
		return "deleted"
	}
	return "unknown"
}

// FromProcessor is the smallest unit of entity who does inorder processing or contains inorder data.
type FromProcessor struct {
	ctx                   context.Context
	entity                processor.ProcessorEntitier
	status                status
	offsetTimeline        *OffsetTimeline
	offsetTimelineWatcher nats.KeyWatcher
	lock                  sync.RWMutex
	log                   *zap.SugaredLogger
}

func (p *FromProcessor) String() string {
	return fmt.Sprintf("status:%v, timeline: %s", p.status, p.offsetTimeline.Dump())
}

func NewProcessor(ctx context.Context, processor processor.ProcessorEntitier, capacity int, watcher nats.KeyWatcher) *FromProcessor {
	p := &FromProcessor{
		ctx:                   ctx,
		entity:                processor,
		status:                _active,
		offsetTimeline:        NewOffsetTimeline(ctx, capacity),
		offsetTimelineWatcher: watcher,
		log:                   logging.FromContext(ctx),
	}
	if watcher != nil {
		go p.startTimeLineWatcher()
	}
	return p
}

func (p *FromProcessor) setStatus(s status) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.status = s
}

// IsActive returns whether a processor is active.
func (p *FromProcessor) IsActive() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _active
}

// IsInactive returns whether a processor is inactive (no heartbeats or any sort).
func (p *FromProcessor) IsInactive() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _inactive
}

// IsDeleted returns whether a processor has been deleted.
func (p *FromProcessor) IsDeleted() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _deleted
}

func (p *FromProcessor) startTimeLineWatcher() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case value := <-p.offsetTimelineWatcher.Updates():
			// TODO: why will value will be nil?
			if value == nil {
				continue
			}
			switch value.Operation() {
			case nats.KeyValuePut:
				epoch, skip, err := p.entity.ParseOTWatcherKey(value.Key())
				if err != nil {
					p.log.Errorw("unable to convert value.Key() to int64", zap.String("received", value.Key()), zap.Error(err))
					continue
				}
				// if skip is set to true, it means the key update we received is for a different processor (sharing of bucket)
				if skip {
					continue
				}
				uint64Value := binary.LittleEndian.Uint64(value.Value())
				p.offsetTimeline.Put(OffsetWatermark{
					watermark: epoch,
					offset:    int64(uint64Value),
				})
				p.log.Debugf("[%s]timelineWatcher- Updates:  [%s] %s > %d: %d", p.entity.GetBucketName(), value.Operation(), value.Bucket(), epoch, int64(uint64Value))
				p.log.Debugf("[%s]%s", p.entity.GetBucketName(), p.offsetTimeline.Dump())
			case nats.KeyValueDelete:
				// we do not care about Delete events because the timeline bucket is meant to grow and the TTL will
				// naturally trim the KV store.
			case nats.KeyValuePurge:
				// skip
			}
		}

	}
}
