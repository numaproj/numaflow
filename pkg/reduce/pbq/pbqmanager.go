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

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
	"github.com/numaproj/numaflow/pkg/window"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// Manager helps in managing the lifecycle of PBQ instances
type Manager struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	storeProvider wal.Manager
	pbqOptions    *options
	pbqMap        map[string]*PBQ
	log           *zap.SugaredLogger
	windowType    window.Type
	// we need lock to access pbqMap, since deregister will be called inside pbq
	// and each pbq will be inside a go routine, and also entire PBQ could be managed
	// through a go routine (depends on the orchestrator)
	sync.RWMutex
}

// NewManager returns new instance of manager
// We don't intend this to be called by multiple routines.
func NewManager(ctx context.Context, vertexName string, pipelineName string, vr int32, storeProvider wal.Manager, windowType window.Type, opts ...PBQOption) (*Manager, error) {
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
		windowType:    windowType,
	}

	return pbqManager, nil
}

// CreateNewPBQ creates new pbq for a partition
func (m *Manager) CreateNewPBQ(ctx context.Context, partitionID partition.ID) (ReadWriteCloser, error) {
	persistentStore, err := m.storeProvider.CreateWAL(ctx, partitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to create a PBQ store, %w", err)
	}

	// output channel is buffered to support bulk reads
	p := &PBQ{
		vertexName:    m.vertexName,
		pipelineName:  m.pipelineName,
		vertexReplica: m.vertexReplica,
		store:         persistentStore,
		output:        make(chan *window.TimedWindowRequest, m.pbqOptions.channelBufferSize),
		cob:           false,
		PartitionID:   partitionID,
		options:       m.pbqOptions,
		manager:       m,
		windowType:    m.windowType, // FIXME(session): this is can be removed when we have unaligned window replay
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
			ctxClosedErr = wait.ExponentialBackoff(PBQCloseBackOff, func() (done bool, err error) {
				closeErr := q.Close()
				if closeErr != nil {
					attempt += 1
					m.log.Errorw("Failed to close pbq, retrying", zap.Any("attempt", attempt), zap.Any("ID", q.PartitionID), zap.Error(closeErr))
					// if ctx is closed, we should return true
					if ctx.Err() != nil {
						return false, ctx.Err()
					}
					// exponential backoff will return if err is not nil
					return false, nil
				}
				m.log.Infow("Successfully closed pbq", zap.String("ID", q.PartitionID.String()))
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
	delete(m.pbqMap, partitionID.String())
	m.Unlock()

	activePartitionCount.With(map[string]string{
		metrics.LabelVertex:             m.vertexName,
		metrics.LabelPipeline:           m.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(m.vertexReplica)),
	}).Dec()

	return m.storeProvider.DeleteWAL(partitionID)
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
