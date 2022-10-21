package fetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
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

// ProcessorToFetch is the smallest unit of entity (from which we fetch data) that does inorder processing or contains inorder data.
type ProcessorToFetch struct {
	ctx            context.Context
	cancel         context.CancelFunc
	entity         processor.ProcessorEntitier
	status         status
	offsetTimeline *OffsetTimeline
	otWatcher      store.WatermarkKVWatcher
	lock           sync.RWMutex
	log            *zap.SugaredLogger
}

func (p *ProcessorToFetch) String() string {
	return fmt.Sprintf("%s status:%v, timeline: %s", p.entity.GetID(), p.getStatus(), p.offsetTimeline.Dump())
}

// NewProcessorToFetch creates ProcessorToFetch.
func NewProcessorToFetch(ctx context.Context, processor processor.ProcessorEntitier, capacity int, watcher store.WatermarkKVWatcher) *ProcessorToFetch {
	ctx, cancel := context.WithCancel(ctx)
	p := &ProcessorToFetch{
		ctx:            ctx,
		cancel:         cancel,
		entity:         processor,
		status:         _active,
		offsetTimeline: NewOffsetTimeline(ctx, capacity),
		otWatcher:      watcher,
		log:            logging.FromContext(ctx),
	}
	go p.startTimeLineWatcher()
	return p
}

func (p *ProcessorToFetch) setStatus(s status) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.status = s
}

func (p *ProcessorToFetch) getStatus() status {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status
}

// IsActive returns whether a processor is active.
func (p *ProcessorToFetch) IsActive() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _active
}

// IsInactive returns whether a processor is inactive (no heartbeats or any sort).
func (p *ProcessorToFetch) IsInactive() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _inactive
}

// IsDeleted returns whether a processor has been deleted.
func (p *ProcessorToFetch) IsDeleted() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status == _deleted
}

func (p *ProcessorToFetch) stopTimeLineWatcher() {
	p.cancel()
}

func (p *ProcessorToFetch) startTimeLineWatcher() {
	watchCh, stopped := p.otWatcher.Watch(p.ctx)

	for {
		select {
		case <-stopped:
			// no need to close ot watcher here because the ot watcher is shared for the given vertex
			// the parent ctx will close the ot watcher
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
					p.log.Errorw("Unable to convert value.PartitionID() to int64", zap.String("received", value.Key()), zap.Error(err))
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
				p.log.Debugw("TimelineWatcher- Updates", zap.String("bucket", p.otWatcher.GetKVName()), zap.Int64("epoch", epoch), zap.Uint64("value", uint64Value))
			case store.KVDelete:
				// we do not care about Delete events because the timeline bucket is meant to grow and the TTL will
				// naturally trim the KV store.
			case store.KVPurge:
				// skip
			}
		}
	}
}
