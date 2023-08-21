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

// package processor contains the logic for managing the processors and their offset timelines.
// It also takes care of managing the lifecycle of the processors by listening to the heartbeat of the processors, and
// also maintains the processor to offset timeline mapping. ProcessorManager will be used by the `fetcher` to
// fetch the active processors list at any given time. ProcessorManager is also responsible for watching the offset
// timeline changes and updating the offset timeline for each processor.

package processor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// ProcessorManager manages the point of view of Vn-1 from Vn vertex processors (or source processor). The code is running on Vn vertex.
// It has the mapping of all the processors which in turn has all the information about each processor timelines.
type ProcessorManager struct {
	ctx       context.Context
	hbWatcher kvs.KVWatcher
	otWatcher kvs.KVWatcher
	// heartbeat just tracks the heartbeat of each processing unit. we use it to mark a processing unit's status (e.g, inactive)
	heartbeat *ProcessorHeartbeat
	// processors has reference to the actual processing unit (ProcessorEntitier) which includes offset timeline which is
	// used for tracking watermark.
	processors map[string]*ProcessorToFetch
	// fromBufferPartitionCount is the number of partitions in the fromBuffer
	fromBufferPartitionCount int32
	lock                     sync.RWMutex
	doneCh                   chan struct{}
	waitCh                   chan struct{}
	log                      *zap.SugaredLogger

	// opts
	opts *processorManagerOptions
}

// NewProcessorManager returns a new ProcessorManager instance
func NewProcessorManager(ctx context.Context, watermarkStoreWatcher store.WatermarkStoreWatcher, fromBufferPartitionCount int32, inputOpts ...ProcessorManagerOption) *ProcessorManager {
	opts := &processorManagerOptions{
		podHeartbeatRate:         5,
		refreshingProcessorsRate: 5,
		isReduce:                 false,
		isSource:                 false,
		vertexReplica:            0,
	}
	for _, opt := range inputOpts {
		opt(opts)
	}
	v := &ProcessorManager{
		ctx:                      ctx,
		hbWatcher:                watermarkStoreWatcher.HeartbeatWatcher(),
		otWatcher:                watermarkStoreWatcher.OffsetTimelineWatcher(),
		heartbeat:                NewProcessorHeartbeat(),
		processors:               make(map[string]*ProcessorToFetch),
		fromBufferPartitionCount: fromBufferPartitionCount,
		log:                      logging.FromContext(ctx),
		doneCh:                   make(chan struct{}),
		waitCh:                   make(chan struct{}),
		opts:                     opts,
	}
	if v.opts.isReduce || v.opts.isSource {
		v.fromBufferPartitionCount = 1
	}
	// start the go routines to watch the heartbeat and offset timeline changes of the processors
	go v.init()
	return v
}

func (v *ProcessorManager) init() {
	var wg = sync.WaitGroup{}
	// start refreshing processors goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		v.startRefreshingProcessors()
	}()

	// start heartbeat watcher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		v.startHeartBeatWatcher()
	}()

	// start offset timeline watcher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		v.startTimeLineWatcher()
	}()

	wg.Wait()
	close(v.waitCh)
}

// AddProcessor adds a new processor. If the given processor already exists, the value will be updated.
func (v *ProcessorManager) AddProcessor(processor string, p *ProcessorToFetch) {
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
		case <-v.doneCh:
			return
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
			// this new processor will be added in the startHeartBeatWatcher() with status=active
			continue
		}
		// default heartbeat rate is every 5 seconds
		// TODO: tolerance?
		if time.Now().Unix()-pTime > 10*v.opts.podHeartbeatRate {
			// if the pod doesn't come back after 10 heartbeats,
			// it's possible the pod has exited unexpectedly, so we need to delete the pod
			// NOTE: the pod entry still remains in the heartbeat store (bucket)
			// TODO: how to delete the pod from the heartbeat store?
			v.log.Infow("Processor has been inactive for 10 heartbeats, deleting...", zap.String("key", pName), zap.String(pName, p.String()))
			p.setStatus(_deleted)
			v.heartbeat.Delete(pName)
		} else if time.Now().Unix()-pTime > v.opts.podHeartbeatRate {
			// if the pod's last heartbeat is greater than podHeartbeatRate
			// then the pod is not considered as live
			p.setStatus(_inactive)
		} else {
			p.setStatus(_active)
			debugStr.WriteString(fmt.Sprintf("[%s] ", pName))
		}
	}
	v.log.Debugw("Active processors", zap.String("hbKVName", v.hbWatcher.GetKVName()), zap.String("otKVName", v.otWatcher.GetKVName()), zap.String("DebugStr", debugStr.String()))
}

// startHeartBeatWatcher starts the processor Heartbeat Watcher to listen to the processor bucket and update the processor
// Heartbeat map. In the processor heartbeat bucket we have the structure key: processor-name, value: processor-heartbeat.
func (v *ProcessorManager) startHeartBeatWatcher() {
	watchCh, stopped := v.hbWatcher.Watch(v.ctx)
	for {
		select {
		case <-stopped:
			return
		case value := <-watchCh:
			if value == nil {
				v.log.Warnw("Received nil value from heartbeat watcher")
				continue
			}
			switch value.Operation() {
			case kvs.KVPut:
				v.log.Debugw("Processor heartbeat watcher received a put", zap.String("key", value.Key()), zap.ByteString("value", value.Value()))
				// do we have such a processor
				p := v.GetProcessor(value.Key())
				if p == nil || p.IsDeleted() {
					// if p is nil, create a new processor
					// A fromProcessor needs to be added to v.processors
					// The fromProcessor may have been deleted
					// TODO: make capacity configurable
					var entity = NewProcessorEntity(value.Key())
					// if the processor is a reduce or source processor, then we only need one fromProcessor
					// because the reduce or source will read from only one partition.
					fromProcessor := NewProcessorToFetch(v.ctx, entity, 10, v.fromBufferPartitionCount)
					v.AddProcessor(value.Key(), fromProcessor)
					v.log.Infow("Successfully added a new fromProcessor", zap.String("fromProcessor", value.Key()))
				} else { // else just make a note that this processor is still active
					p.setStatus(_active)
				}
				// value is epoch
				intValue, convErr := strconv.Atoi(string(value.Value()))
				if convErr != nil {
					v.log.Errorw("Unable to convert intValue.WMB() to int64", zap.Error(convErr))
				} else {
					// insert the last seen timestamp. we use this to figure whether this processor entity is inactive.
					v.heartbeat.Put(value.Key(), int64(intValue))
				}
			case kvs.KVDelete:
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
			case kvs.KVPurge:
				v.log.Warnw("Received nats.KeyValuePurge", zap.String("kvName", v.hbWatcher.GetKVName()))
			}
			v.log.Debugw("processorHeartbeatWatcher - Updates:", zap.String("operation", value.Operation().String()),
				zap.String("hbKVName", v.hbWatcher.GetKVName()), zap.String("key", value.Key()), zap.ByteString("value", value.Value()))
		}

	}
}

// startTimeLineWatcher starts the processor Timeline Watcher to listen to offset timeline bucket and update the processor's timeline.
func (v *ProcessorManager) startTimeLineWatcher() {
	watchCh, stopped := v.otWatcher.Watch(v.ctx)

	for {
		select {
		case <-stopped:
			return
		case value := <-watchCh:
			if value == nil {
				continue
			}
			switch value.Operation() {
			case kvs.KVPut:
				v.log.Debugw("Offset timeline watcher received a put", zap.String("key", value.Key()), zap.Binary("b64Value", value.Value()))
				// a new processor's OT might take up to 5 secs to be reflected because we are not waiting for it to be added.
				// This should not be a problem because the processor will send heartbeat as soon as it boots up.
				// In case we miss it, we might see a delay.
				p := v.GetProcessor(value.Key())
				if p == nil {
					v.log.Errorw("Unable to find the processor", zap.String("processorEntity", value.Key()))
					continue
				}
				otValue, err := wmb.DecodeToWMB(value.Value())
				// if it's a reduce vertex, consider the watermarks only for the partition that the processor is running on. this is because
				// reduce processors has 1:1 relation with partitions.
				// also, ignore all events not belonging to this partition.
				if v.opts.isReduce && otValue.Partition != v.opts.vertexReplica {
					continue
				}
				if err != nil {
					v.log.Errorw("Unable to decode the value", zap.String("processorEntity", p.entity.GetName()), zap.Error(err))
					continue
				}
				if otValue.Idle {
					if v.opts.isReduce {
						p.offsetTimelines[0].PutIdle(otValue)
					} else {
						p.offsetTimelines[otValue.Partition].PutIdle(otValue)
					}
					v.log.Debugw("TimelineWatcher- Updates", zap.String("otKVName", v.otWatcher.GetKVName()), zap.Int64("idleWatermark", otValue.Watermark), zap.Int64("idleOffset", otValue.Offset))
				} else {
					// NOTE: currently, for source edges, the otValue.Idle is always false
					// for reduce we will always have only one partition
					if v.opts.isReduce {
						p.offsetTimelines[0].Put(otValue)
					} else {
						p.offsetTimelines[otValue.Partition].Put(otValue)
					}
					v.log.Debugw("TimelineWatcher- Updates", zap.String("otKVName", v.otWatcher.GetKVName()), zap.Int64("watermark", otValue.Watermark), zap.Int64("offset", otValue.Offset))
				}
			case kvs.KVDelete:
				// we do not care about Delete events because the timeline bucket is meant to grow and the TTL will
				// naturally trim the KV store.
			case kvs.KVPurge:
				// skip
			}
		}
	}
}

// Close stops the watchers, and waits for the goroutines to exit.
func (v *ProcessorManager) Close() {
	// send a signal to the goroutines to exit
	close(v.doneCh)

	// close the watchers
	v.hbWatcher.Close()
	v.otWatcher.Close()

	// wait for the goroutines to exit
	<-v.waitCh
}
