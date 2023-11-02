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

package pbq

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store"
	"github.com/numaproj/numaflow/pkg/window"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// registeredWindow to track the number of windows for a start and end time.
type registeredWindow struct {
	startTime time.Time
	endTime   time.Time
	// count how many slots do we have for this window. currently we only use slot-0, so count is always 1
	count *atomic.Int32
}

var _ window.AlignedWindower = (*registeredWindow)(nil)

func (r *registeredWindow) StartTime() time.Time {
	return r.startTime
}

func (r *registeredWindow) EndTime() time.Time {
	return r.endTime
}

// Manager helps in managing the lifecycle of PBQ instances
type Manager struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	storeProvider store.StoreProvider
	pbqOptions    *options
	pbqMap        map[string]*PBQ
	log           *zap.SugaredLogger
	yetToBeClosed *window.SortedWindowList[*registeredWindow]
	// we need lock to access pbqMap, since deregister will be called inside pbq
	// and each pbq will be inside a go routine, and also entire PBQ could be managed
	// through a go routine (depends on the orchestrator)
	sync.RWMutex
}

// NewManager returns new instance of manager
// We don't intend this to be called by multiple routines.
func NewManager(ctx context.Context, vertexName string, pipelineName string, vr int32, storeProvider store.StoreProvider, opts ...PBQOption) (*Manager, error) {
	pbqOpts := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			if err := opt(pbqOpts); err != nil {
				return nil, err
			}
		}
	}

	pbqManager := &Manager{
		vertexName:    vertexName,
		pipelineName:  pipelineName,
		vertexReplica: vr,
		storeProvider: storeProvider,
		pbqMap:        make(map[string]*PBQ),
		pbqOptions:    pbqOpts,
		log:           logging.FromContext(ctx),
		yetToBeClosed: window.NewSortedWindowList[*registeredWindow](),
	}

	return pbqManager, nil
}

// CreateNewPBQ creates new pbq for a partition
func (m *Manager) CreateNewPBQ(ctx context.Context, partitionID partition.ID) (ReadWriteCloser, error) {
	persistentStore, err := m.storeProvider.CreateStore(ctx, partitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to create a PBQ store, %w", err)
	}

	// output channel is buffered to support bulk reads
	p := &PBQ{
		vertexName:    m.vertexName,
		pipelineName:  m.pipelineName,
		vertexReplica: m.vertexReplica,
		store:         persistentStore,
		output:        make(chan *isb.ReadMessage),
		cob:           false,
		PartitionID:   partitionID,
		options:       m.pbqOptions,
		manager:       m,
		log:           logging.FromContext(ctx).With("PBQ", partitionID),
	}
	m.register(partitionID, p)
	return p, nil
}

// ListPartitions returns all the pbq instances
func (m *Manager) ListPartitions() []*PBQ {
	m.RLock()
	defer m.RUnlock()

	pbqList := make([]*PBQ, len(m.pbqMap))
	i := 0
	for _, val := range m.pbqMap {
		pbqList[i] = val
		i++
	}

	return pbqList
}

// GetPBQ returns pbq for the given ID
func (m *Manager) GetPBQ(partitionID partition.ID) ReadWriteCloser {
	m.RLock()
	defer m.RUnlock()

	if pbqInstance, ok := m.pbqMap[partitionID.String()]; ok {
		return pbqInstance
	}

	return nil
}

// GetExistingPartitions restores the state of the pbqManager. It reads from the PBQs store to get the persisted partitions
// and builds the PBQ Map.
func (m *Manager) GetExistingPartitions(ctx context.Context) ([]partition.ID, error) {
	var ctxClosedErr error
	var partitionIDs []partition.ID

	var discoverPartitionsBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 100 * time.Millisecond,
		Factor:   1,
		Jitter:   0.1,
	}

	ctxClosedErr = wait.ExponentialBackoffWithContext(ctx, discoverPartitionsBackoff, func() (done bool, err error) {
		var attempt int

		partitionIDs, err = m.storeProvider.DiscoverPartitions(ctx)
		if err != nil {
			attempt += 1
			m.log.Errorw("Failed to discover partitions during startup, retrying", zap.Any("attempt", attempt), zap.Error(err))
			return false, nil
		}
		return true, nil
	})
	if ctxClosedErr != nil {
		m.log.Errorw("Context closed while discovering partitions", zap.Error(ctxClosedErr))
		return partitionIDs, ctxClosedErr
	}

	return partitionIDs, nil
}

// ShutDown for clean shut down, flushes pending messages to store and closes the store
func (m *Manager) ShutDown(ctx context.Context) {
	// iterate through the map of pbq
	// close all the pbq
	var wg sync.WaitGroup
	var PBQCloseBackOff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 100 * time.Millisecond,
		Factor:   1,
		Jitter:   0.1,
	}

	for _, v := range m.getPBQs() {
		wg.Add(1)
		go func(q *PBQ) {
			defer wg.Done()
			var ctxClosedErr error
			var attempt int
			ctxClosedErr = wait.ExponentialBackoffWithContext(ctx, PBQCloseBackOff, func() (done bool, err error) {
				closeErr := q.Close()
				if closeErr != nil {
					attempt += 1
					m.log.Errorw("Failed to close pbq, retrying", zap.Any("attempt", attempt), zap.Any("ID", q.PartitionID), zap.Error(closeErr))
					// exponential backoff will return if err is not nil
					return false, nil
				}
				return true, nil
			})
			if ctxClosedErr != nil {
				m.log.Errorw("Context closed while closing pbq", zap.Any("ID", q.PartitionID), zap.Error(ctxClosedErr))
			}
		}(v)
	}

	wg.Wait()
}

// register is intended to be used by PBQ to register itself with the manager.
func (m *Manager) register(partitionID partition.ID, p *PBQ) {
	ww := &registeredWindow{
		startTime: partitionID.Start,
		endTime:   partitionID.End,
		count:     atomic.NewInt32(0),
	}

	ww, _ = m.yetToBeClosed.InsertIfNotPresent(ww)
	ww.count.Add(1)

	m.Lock()
	defer m.Unlock()

	if _, ok := m.pbqMap[partitionID.String()]; !ok {
		m.pbqMap[partitionID.String()] = p
	}
	activePartitionCount.With(map[string]string{
		metrics.LabelVertex:             m.vertexName,
		metrics.LabelPipeline:           m.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(m.vertexReplica)),
	}).Inc()

}

// deregister is intended to be used by PBQ to deregister itself after GC is called.
// it will also delete the store using the store provider
func (m *Manager) deregister(partitionID partition.ID) error {

	m.Lock()
	ww := &registeredWindow{
		startTime: partitionID.Start,
		endTime:   partitionID.End,
		count:     atomic.NewInt32(0),
	}

	delete(m.pbqMap, partitionID.String())
	m.Unlock()

	// update yetToBeClosed list
	ww, _ = m.yetToBeClosed.InsertIfNotPresent(ww)
	ww.count.Sub(1)

	// if count is 0, delete from yetToBeClosed list
	if ww.count.Load() == 0 {
		m.yetToBeClosed.DeleteWindow(ww)
	}

	activePartitionCount.With(map[string]string{
		metrics.LabelVertex:             m.vertexName,
		metrics.LabelPipeline:           m.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(m.vertexReplica)),
	}).Dec()

	return m.storeProvider.DeleteStore(partitionID)
}

func (m *Manager) getPBQs() []*PBQ {
	m.RLock()
	defer m.RUnlock()
	var pbqs = make([]*PBQ, 0, len(m.pbqMap))
	for _, pbq := range m.pbqMap {
		pbqs = append(pbqs, pbq)
	}

	return pbqs
}

// Replay replays messages which are persisted in pbq store.
func (m *Manager) Replay(ctx context.Context) {
	var wg sync.WaitGroup
	var tm = time.Now()
	partitionsIds := make([]partition.ID, 0)
	for _, val := range m.getPBQs() {
		partitionsIds = append(partitionsIds, val.PartitionID)
		wg.Add(1)
		m.log.Info("Replaying records from store", zap.Any("PBQ", val.PartitionID))
		go func(ctx context.Context, p *PBQ) {
			defer wg.Done()
			p.replayRecordsFromStore(ctx)
		}(ctx, val)
	}

	wg.Wait()
	m.log.Infow("Finished replaying records from store", zap.Duration("took", time.Since(tm)), zap.Any("partitions", partitionsIds))
}

// NextWindowToBeMaterialized returns the next keyed window that is yet to be materialized(GCed)
// will be used by the data forwarder to publish the idle watermark. While publishing idle watermark, we have to be
// conservative. PBQManager's view of next window to be materialized is conservative as it is on the reading side.
// We SHOULD NOT use NextWindowToBeMaterialized to write data to because it could fail (channel could have been closed but
// GC is yet to happen), this function should only be on readonly path.
func (m *Manager) NextWindowToBeMaterialized() window.AlignedWindower {
	if m.yetToBeClosed.Len() == 0 {
		return nil
	}
	return m.yetToBeClosed.Front()
}
