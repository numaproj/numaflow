package pbq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
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
func (m *Manager) CreateNewPBQ(ctx context.Context, partitionID string) (ReadWriteCloser, error) {

	var persistentStore store.Store
	var err error

	switch m.storeOptions.PbqStoreType() {
	case dfv1.NoOpType:
		persistentStore, _ = noop.NewPBQNoOpStore()
	case dfv1.InMemoryType:
		persistentStore, err = memory.NewMemoryStore(ctx, partitionID, m.storeOptions)
		if err != nil {
			m.log.Errorw("Error while creating persistent store", zap.Any("partitionID", partitionID), zap.Any("storeType", m.storeOptions.PbqStoreType()), zap.Error(err))
			return nil, err
		}
	case dfv1.FileSystemType:
		return nil, errors.New("not implemented")
	}

	// output channel is buffered to support bulk reads
	p := &PBQ{
		store:       persistentStore,
		output:      make(chan *isb.Message, m.pbqOptions.channelBufferSize),
		cob:         false,
		partitionID: partitionID,
		options:     m.pbqOptions,
		manager:     m,
		log:         logging.FromContext(ctx).With("PBQ", partitionID),
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

// GetPBQ returns pbq for the given partitionID
func (m *Manager) GetPBQ(partitionID string) ReadWriteCloser {
	m.RLock()
	defer m.RUnlock()

	if pbqInstance, ok := m.pbqMap[partitionID]; ok {
		return pbqInstance
	}

	return nil
}

// StartUp restores the state of the pbqManager
func (m *Manager) StartUp(_ context.Context) error {

	switch m.storeOptions.PbqStoreType() {
	case dfv1.NoOpType:
		return nil
	case dfv1.InMemoryType:
		return nil
	case dfv1.FileSystemType:
		return fmt.Errorf("not implemented store type %s", m.storeOptions.PbqStoreType())
	default:
		return fmt.Errorf("unknown store type %s", m.storeOptions.PbqStoreType())
	}

}

// ShutDown for clean shut down, flushes pending messages to store and closes the store
func (m *Manager) ShutDown(ctx context.Context) {
	// iterate through the map of pbq
	// close all the pbq
	var wg sync.WaitGroup

	var PBQCloseBackOff = wait.Backoff{
		Steps:    5,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
		Cap:      5 * time.Second,
	}

	for _, v := range m.pbqMap {
		wg.Add(1)
		go func(q *PBQ) {
			defer wg.Done()
			var closeErr error
			var attempt int
			closeErr = wait.ExponentialBackoffWithContext(ctx, PBQCloseBackOff, func() (done bool, err error) {
				closeErr = q.Close()
				if closeErr != nil {
					attempt += 1
					m.log.Warnw("Failed to close pbq, retrying", zap.Any("attempt", attempt), zap.Any("partitionID", q.partitionID), zap.Error(closeErr))
					// exponential backoff will return if err is not nil
					return false, nil
				}
				return true, nil
			})
			if closeErr != nil {
				m.log.Errorw("Failed to close pbq, no retries left", zap.Any("partitionID", q.partitionID), zap.Error(closeErr))
			}
		}(v)
	}

	wg.Wait()
}

// register is intended to be used by PBQ to register itself with the manager.
func (m *Manager) register(partitionID string, p *PBQ) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.pbqMap[partitionID]; !ok {
		m.pbqMap[partitionID] = p
	}
}

// deregister is intended to be used by PBQ to deregister itself after GC is called.
func (m *Manager) deregister(partitionID string) {
	m.Lock()
	defer m.Unlock()

	delete(m.pbqMap, partitionID)
}

// Replay replays messages which are persisted in pbq store
func (m *Manager) Replay(ctx context.Context) {
	var wg sync.WaitGroup

	for _, val := range m.pbqMap {
		wg.Add(1)
		m.log.Info("Replaying records from store", zap.Any("PBQ", val.partitionID))
		go func(ctx context.Context, p *PBQ) {
			defer wg.Done()
			p.replayRecordsFromStore(ctx)
		}(ctx, val)
	}

	wg.Wait()
}
