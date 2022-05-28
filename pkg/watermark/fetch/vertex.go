package fetch

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"go.uber.org/zap"
)

// FromVertexer defines an interface which builds the view of Vn-th vertex from the point of view of Vn-th vertex.
type FromVertexer interface {
	// GetAllProcessors fetches all the processors from Vn-1 vertex. processors could be pods or when the vertex is a
	// source vertex, it could be partitions if the source is Kafka.
	GetAllProcessors() map[string]*FromProcessor
}

// FromVertex is the point of view of Vn-1 from Vn vertex. The code is running on Vn vertex.
// It has the mapping of all the processors which in turn has all the information about each processor
// timelines.
type FromVertex struct {
	ctx context.Context
	js  nats.JetStreamContext
	// keyspace is only used if we have a processorHBWatcher
	keyspace           string
	processorHBWatcher nats.KeyWatcher
	heartbeat          *ProcessorHeartbeat
	processors         map[string]*FromProcessor
	lock               sync.RWMutex
	log                *zap.SugaredLogger

	// opts
	opts *vertexOptions
}

// NewFromVertex returns `FromVertex`
func NewFromVertex(ctx context.Context, keyspace string, js nats.JetStreamContext, heartbeatWatcher nats.KeyWatcher, inputOpts ...VertexOption) *FromVertex {
	opts := &vertexOptions{
		podHeartbeatRate:         5,
		refreshingProcessorsRate: 5,
		separateOTBucket:         false,
	}
	for _, opt := range inputOpts {
		opt(opts)
	}
	v := &FromVertex{
		ctx:                ctx,
		js:                 js,
		keyspace:           keyspace,
		processorHBWatcher: heartbeatWatcher,
		heartbeat:          NewProcessorHeartbeat(),
		processors:         make(map[string]*FromProcessor),
		log:                logging.FromContext(ctx),
		opts:               opts,
	}
	go v.startRefreshingProcessors()
	// we do not care about heartbeat watcher if this is a source vertex
	if v.processorHBWatcher != nil {
		go v.startHeatBeatWatcher()
	}
	return v
}

// AddProcessor adds a new processor.
func (v *FromVertex) AddProcessor(processor string, p *FromProcessor) {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.processors[processor] = p
}

// GetProcessor gets a processor.
func (v *FromVertex) GetProcessor(processor string) *FromProcessor {
	v.lock.RLock()
	defer v.lock.RUnlock()
	if p, ok := v.processors[processor]; ok {
		return p
	}
	return nil
}

// DeleteProcessor deletes a processor.
func (v *FromVertex) DeleteProcessor(processor string) {
	v.lock.Lock()
	defer v.lock.Unlock()
	// we do not require this processor's reference anymore
	delete(v.processors, processor)
}

// GetAllProcessors returns all the processors.
func (v *FromVertex) GetAllProcessors() map[string]*FromProcessor {
	v.lock.RLock()
	defer v.lock.RUnlock()
	var processors = make(map[string]*FromProcessor, len(v.processors))
	for k, v := range v.processors {
		processors[k] = v
	}
	return processors
}

func (v *FromVertex) startRefreshingProcessors() {
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
func (v *FromVertex) refreshingProcessors() {
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
	v.log.Debugf("[%s] active processors %+v", v.keyspace, debugStr.String())
}

// startHeatBeatWatcher starts the processor Heartbeat Watcher to listen to the processor bucket and update the processor
// Heartbeat map. In the processor heartbeat bucket we have the structure key: processor-name, value: processor-heartbeat.
func (v *FromVertex) startHeatBeatWatcher() {
	for {
		select {
		case <-v.ctx.Done():
			return
		case value := <-v.processorHBWatcher.Updates():
			if value == nil {
				continue
			}
			switch value.Operation() {
			case nats.KeyValuePut:
				var entity = processor.NewProcessorEntity(value.Key(), v.keyspace, processor.WithSeparateOTBuckets(v.opts.separateOTBucket))
				p := v.GetProcessor(value.Key())
				if p == nil {
					// A fromProcessor need to be added to v.processors
					watcher := v.buildNewProcessorWatcher(entity.GetBucketName())
					// The fromProcessor may have been deleted
					// TODO: how to implement a reconciler where the timeline bucket is there but we can't create the watcher
					if watcher == nil {
						v.log.Errorw("unable to create the watcher for fromProcessor", zap.String("fromProcessor", value.Key()))
						continue
					}
					var fromProcessor = NewProcessor(v.ctx, entity, 5, watcher)
					v.AddProcessor(value.Key(), fromProcessor)
					v.log.Infow("v.AddProcessor successfully added a new fromProcessor", zap.String("fromProcessor", value.Key()))
				} else {
					p.setStatus(_active)
				}
				// update to v.heartbeat
				intValue, convErr := strconv.Atoi(string(value.Value()))
				if convErr != nil {
					v.log.Errorw("unable to convert intValue.Value() to int64", zap.Error(convErr))
				}
				v.heartbeat.Put(value.Key(), int64(intValue))
			case nats.KeyValueDelete:
				p := v.GetProcessor(value.Key())
				if p == nil {
					v.log.Errorw("nil pointer for the processor, perhaps already deleted", zap.String("key", value.Key()))
				} else if p.IsDeleted() {
					v.log.Warnw("already deleted", zap.String("key", value.Key()), zap.String(value.Key(), p.String()))
				} else {
					v.log.Infow("deleting", zap.String("key", value.Key()), zap.String(value.Key(), p.String()))
					p.setStatus(_deleted)
					v.heartbeat.Delete(value.Key())
				}
			case nats.KeyValuePurge:
				v.log.Errorw("received nats.KeyValuePurge", zap.String("bucket", value.Bucket()))
			}
			v.log.Debugf("[%s] podHeartbeatWatcher - Updates: [%s] %s > %s: %s", v.keyspace, value.Operation(), value.Bucket(), value.Key(), value.Value())
		}

	}
}

func (v *FromVertex) buildNewProcessorWatcher(bucketName string) nats.KeyWatcher {
	return RetryUntilSuccessfulWatcherCreation(v.js, bucketName, false, v.log)
}
