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
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/store/memory"
	"github.com/numaproj/numaflow/pkg/pbq/store/noop"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Manager helps in managing the lifecycle of PBQ instances
type Manager struct {
	storeOptions *store.StoreOptions
	pbqOptions   *options
	pbqMap       map[string]*PBQ
	log          *zap.SugaredLogger
	// we need lock to access pbqMap, since deregister will be called inside pbq
	// and each pbq will be inside a go routine, and also entire PBQ could be managed
	// through a go routine (depends on the orchestrator)
	sync.RWMutex
}

// NewManager returns new instance of manager
// We don't intend this to be called by multiple routines.
func NewManager(ctx context.Context, opts ...PBQOption) (*Manager, error) {
	pbqOpts := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			if err := opt(pbqOpts); err != nil {
				return nil, err
			}
		}
	}

	pbqManager := &Manager{
		pbqMap:       make(map[string]*PBQ),
		pbqOptions:   pbqOpts,
		storeOptions: pbqOpts.storeOptions,
		log:          logging.FromContext(ctx),
	}

	return pbqManager, nil
}

// CreateNewPBQ creates new pbq for a partition
func (m *Manager) CreateNewPBQ(ctx context.Context, partitionID partition.ID) (ReadWriteCloser, error) {

	var persistentStore store.Store
	var err error

	switch m.storeOptions.PBQStoreType() {
	case dfv1.NoOpType:
		persistentStore, _ = noop.NewPBQNoOpStore()
	case dfv1.InMemoryType:
		persistentStore, err = memory.NewMemoryStore(ctx, partitionID, m.storeOptions)
		if err != nil {
			m.log.Errorw("Error while creating persistent store", zap.Any("ID", partitionID), zap.Any("storeType", m.storeOptions.PBQStoreType()), zap.Error(err))
			return nil, err
		}
	case dfv1.FileSystemType:
		return nil, fmt.Errorf("not implemented, %s", dfv1.FileSystemType)
	default:
		return nil, errors.New("not implemented (default)")
	}

	// output channel is buffered to support bulk reads
	p := &PBQ{
		store:       persistentStore,
		output:      make(chan *isb.ReadMessage, m.pbqOptions.channelBufferSize),
		cob:         false,
		PartitionID: partitionID,
		options:     m.pbqOptions,
		manager:     m,
		log:         logging.FromContext(ctx).With("PBQ", partitionID),
	}

	m.register(partitionID, p)
	return p, nil
}

// discoverPartitions discovers partitions.
func (m *Manager) discoverPartitions(ctx context.Context) ([]partition.ID, error) {
	switch m.storeOptions.PBQStoreType() {
	case dfv1.NoOpType:
		return noop.DiscoverPartitions(ctx, m.storeOptions)
	case dfv1.InMemoryType:
		return memory.DiscoverPartitions(ctx, m.storeOptions)
	case dfv1.FileSystemType:
		return nil, fmt.Errorf("not implemented, %s", dfv1.FileSystemType)
	default:
		return nil, errors.New("not implemented (default)")
	}
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

// StartUp restores the state of the pbqManager. It reads from the PBQs store to get the persisted partitions
// and builds the PBQ Map.
func (m *Manager) StartUp(ctx context.Context) {
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

		partitionIDs, err = m.discoverPartitions(ctx)
		if err != nil {
			attempt += 1
			m.log.Errorw("Failed to discover partitions during startup, retrying", zap.Any("attempt", attempt), zap.Error(err))
			return false, nil
		}
		return true, nil
	})
	if ctxClosedErr != nil {
		m.log.Errorw("Context closed while discovering partitions", zap.Error(ctxClosedErr))
		return
	}

	var createPBQBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 100 * time.Millisecond,
		Factor:   1,
		Jitter:   0.1,
	}

	for _, partitionID := range partitionIDs {
		ctxClosedErr = wait.ExponentialBackoffWithContext(ctx, createPBQBackoff, func() (done bool, err error) {
			var attempt int
			_, err = m.CreateNewPBQ(ctx, partitionID)

			if err != nil {
				attempt += 1
				m.log.Errorw("Failed to create pbq during startup, retrying", zap.Any("attempt", attempt), zap.Any("partitionID", partitionID.String()), zap.Error(err))
				return false, nil
			}
			return true, nil
		})
		if ctxClosedErr != nil {
			m.log.Errorw("Context closed while creating new pbq", zap.Any("partitionID", partitionID.String()), zap.Error(ctxClosedErr))
		}
	}
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

	for _, v := range m.pbqMap {
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
	m.Lock()
	defer m.Unlock()

	if _, ok := m.pbqMap[partitionID.String()]; !ok {
		m.pbqMap[partitionID.String()] = p
	}
}

// deregister is intended to be used by PBQ to deregister itself after GC is called.
func (m *Manager) deregister(partitionID partition.ID) {
	m.Lock()
	defer m.Unlock()

	delete(m.pbqMap, partitionID.String())
}

// Replay replays messages which are persisted in pbq store.
func (m *Manager) Replay(ctx context.Context) {
	var wg sync.WaitGroup

	for _, val := range m.pbqMap {
		wg.Add(1)
		m.log.Info("Replaying records from store", zap.Any("PBQ", val.PartitionID))
		go func(ctx context.Context, p *PBQ) {
			defer wg.Done()
			p.replayRecordsFromStore(ctx)
		}(ctx, val)
	}

	wg.Wait()
}
