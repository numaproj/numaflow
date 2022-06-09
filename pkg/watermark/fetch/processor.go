package fetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"sync"

	"github.com/numaproj/numaflow/pkg/watermark/processor"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
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
	ctx            context.Context
	entity         processor.ProcessorEntitier
	status         status
	offsetTimeline *OffsetTimeline
	otWatcher      store.WatermarkKVWatcher
	lock           sync.RWMutex
	log            *zap.SugaredLogger
}

func (p *FromProcessor) String() string {
	return fmt.Sprintf("%s status:%v, timeline: %s", p.entity.GetID(), p.status, p.offsetTimeline.Dump())
}

func NewProcessor(ctx context.Context, processor processor.ProcessorEntitier, capacity int, watcher store.WatermarkKVWatcher) *FromProcessor {
	p := &FromProcessor{
		ctx:            ctx,
		entity:         processor,
		status:         _active,
		offsetTimeline: NewOffsetTimeline(ctx, capacity),
		otWatcher:      watcher,
		log:            logging.FromContext(ctx),
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
	watchCh := p.otWatcher.Watch(p.ctx)
	for {
		select {
		case <-p.ctx.Done():
			return
		case value := <-watchCh:
			// TODO: why will value will be nil?
			if value == nil {
				continue
			}
			switch value.Operation() {
			case store.KVPut:
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
				p.log.Debugf("[%s]timelineWatcher- Updates:  [%s] > %d: %d", p.entity.GetBucketName(), p.otWatcher.GetKVName(), epoch, int64(uint64Value))
				p.log.Debugf("[%s]%s", p.entity.GetBucketName(), p)
			case store.KVDelete:
				// we do not care about Delete events because the timeline bucket is meant to grow and the TTL will
				// naturally trim the KV store.
			case store.KVPurge:
				// skip
			}
		}

	}
}
