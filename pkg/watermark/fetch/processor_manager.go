package fetch

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

// ProcessorManager manages the point of view of Vn-1 from Vn vertex processors (or source processor). The code is running on Vn vertex.
// It has the mapping of all the processors which in turn has all the information about each processor
// timelines.
type ProcessorManager struct {
	ctx        context.Context
	hbWatcher  store.WatermarkKVWatcher
	otWatcher  store.WatermarkKVWatcher
	heartbeat  *ProcessorHeartbeat
	processors map[string]*ProcessorToFetch
	lock       sync.RWMutex
	log        *zap.SugaredLogger

	// opts
	opts *processorManagerOptions
}

// NewProcessorManager returns a new ProcessorManager instance
func NewProcessorManager(ctx context.Context, hbWatcher store.WatermarkKVWatcher, otWatcher store.WatermarkKVWatcher, inputOpts ...ProcessorManagerOption) *ProcessorManager {
	opts := &processorManagerOptions{
		podHeartbeatRate:         5,
		refreshingProcessorsRate: 5,
		separateOTBucket:         false,
	}
	for _, opt := range inputOpts {
		opt(opts)
	}

	v := &ProcessorManager{
		ctx:        ctx,
		hbWatcher:  hbWatcher,
		otWatcher:  otWatcher,
		heartbeat:  NewProcessorHeartbeat(),
		processors: make(map[string]*ProcessorToFetch),
		log:        logging.FromContext(ctx),
		opts:       opts,
	}
	go v.startRefreshingProcessors()
	// we do not care about heartbeat watcher if this is a source vertex
	if v.hbWatcher != nil {
		go v.startHeatBeatWatcher()
	}
	return v
}

// addProcessor adds a new processor.
func (v *ProcessorManager) addProcessor(processor string, p *ProcessorToFetch) {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.processors[processor] = p
}

// GetProcessor gets a processor.
func (v *ProcessorManager) GetProcessor(processor string) *ProcessorToFetch {
	v.lock.RLock()
	defer v.lock.RUnlock()
	if p, ok := v.processors[processor]; ok {
		return p
	}
	return nil
}

// DeleteProcessor deletes a processor.
func (v *ProcessorManager) DeleteProcessor(processor string) {
	v.lock.Lock()
	defer v.lock.Unlock()
	// we do not require this processor's reference anymore
	delete(v.processors, processor)
}

// GetAllProcessors returns all the processors.
func (v *ProcessorManager) GetAllProcessors() map[string]*ProcessorToFetch {
	v.lock.RLock()
	defer v.lock.RUnlock()
	var processors = make(map[string]*ProcessorToFetch, len(v.processors))
	for k, v := range v.processors {
		processors[k] = v
	}
	return processors
}

func (v *ProcessorManager) startRefreshingProcessors() {
	ticker := time.NewTicker(time.Duration(v.opts.refreshingProcessorsRate) * time.Second)
	defer ticker.Stop()
	v.log.Infow("Refreshing ActiveProcessors ticker started")
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-ticker.C:
			v.refreshingProcessors()
		}
	}
}

// refreshingProcessors keeps the v.ActivePods to be a map of live ActivePods
func (v *ProcessorManager) refreshingProcessors() {
	var debugStr strings.Builder
	for pName, pTime := range v.heartbeat.GetAll() {
		p := v.GetProcessor(pName)
		if p == nil {
			// processor hasn't been added to v.processors yet
			// this new processor will be added in the startHeatBeatWatcher() with status=active
			continue
		}
		// default heartbeat rate is every 5 seconds
		// TODO: tolerance?
		if time.Now().Unix()-pTime > v.opts.podHeartbeatRate {
			// if the pod's last heartbeat is greater than podHeartbeatRate
			// then the pod is not considered as live
			p.setStatus(_inactive)
		} else {
			p.setStatus(_active)
			debugStr.WriteString(fmt.Sprintf("[%s] ", pName))
		}
	}
	v.log.Debugw("Active processors", zap.String("HB", v.hbWatcher.GetKVName()), zap.String("OT", v.otWatcher.GetKVName()), zap.String("DebugStr", debugStr.String()))
}

// startHeatBeatWatcher starts the processor Heartbeat Watcher to listen to the processor bucket and update the processor
// Heartbeat map. In the processor heartbeat bucket we have the structure key: processor-name, value: processor-heartbeat.
func (v *ProcessorManager) startHeatBeatWatcher() {
	watchCh := v.hbWatcher.Watch(v.ctx)
	for {
		select {
		case <-v.ctx.Done():
			return
		case value := <-watchCh:
			if value == nil {
				continue
			}
			switch value.Operation() {
			case store.KVPut:
				var entity = processor.NewProcessorEntity(value.Key(), processor.WithSeparateOTBuckets(v.opts.separateOTBucket))
				// do we have such a processor
				p := v.GetProcessor(value.Key())
				if p == nil { // if no, create a new processor
					// A fromProcessor need to be added to v.processors
					// The fromProcessor may have been deleted
					// TODO: make capacity configurable
					var fromProcessor = NewProcessorToFetch(v.ctx, entity, 10, v.otWatcher)
					v.addProcessor(value.Key(), fromProcessor)
					v.log.Infow("v.AddProcessor successfully added a new fromProcessor", zap.String("fromProcessor", value.Key()))
				} else { // else just make a note that this processor is still active
					p.setStatus(_active)
				}
				// value is epoch
				intValue, convErr := strconv.Atoi(string(value.Value()))
				if convErr != nil {
					v.log.Errorw("Unable to convert intValue.Value() to int64", zap.Error(convErr))
				} else {
					// insert the last seen timestamp. we use this to figure whether this processor entity is inactive.
					v.heartbeat.Put(value.Key(), int64(intValue))
				}
			case store.KVDelete:
				p := v.GetProcessor(value.Key())
				if p == nil {
					v.log.Infow("Nil pointer for the processor, perhaps already deleted", zap.String("key", value.Key()))
				} else if p.IsDeleted() {
					v.log.Warnw("Already deleted", zap.String("key", value.Key()), zap.String(value.Key(), p.String()))
				} else {
					v.log.Infow("Deleting", zap.String("key", value.Key()), zap.String(value.Key(), p.String()))
					p.setStatus(_deleted)
					v.heartbeat.Delete(value.Key())
				}
			case store.KVPurge:
				v.log.Warnw("Received nats.KeyValuePurge", zap.String("bucket", v.hbWatcher.GetKVName()))
			}
			v.log.Debugw("processorHeartbeatWatcher - Updates:", zap.String("Operation", value.Operation().String()),
				zap.String("HB", v.hbWatcher.GetKVName()), zap.String("key", value.Key()), zap.String("value", string(value.Value())))
		}

	}
}
