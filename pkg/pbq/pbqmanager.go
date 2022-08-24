package pbq

import (
	"context"
	"errors"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/store/memory"
	"sync"
)

var NonExistentPBQError error = errors.New("missing PBQ for the partition")

// Manager helps in managing the lifecycle of PBQ instances
type Manager struct {
	options *store.Options
	pbqMap  map[string]*PBQ
}

var createManagerOnce sync.Once
var pbqManager *Manager
var createManagerError error

// * create options during create manager and pass the options around ?

// NewManager returns new instance of manager
// We dont intend this to be called by multiple routines.
func NewManager(opts ...store.SetOption) (*Manager, error) {
	options := store.DefaultOptions()
	createManagerOnce.Do(func() {
		for _, opt := range opts {
			if opt != nil {
				if err := opt(options); err != nil {
					createManagerError = err
					return
				}
			}
		}
		pbqManager = &Manager{
			pbqMap:  make(map[string]*PBQ),
			options: options,
		}
	})
	return pbqManager, createManagerError
}

// ListPartitions returns all the pbq instances
func (m *Manager) ListPartitions() []*PBQ {

	pbqList := make([]*PBQ, len(m.pbqMap))
	i := 0
	for _, val := range m.pbqMap {
		pbqList[i] = val
		i++
	}
	return pbqList
}

// GetPBQ returns pbq for the given partitionID, if createIfMissing is set to true
// it creates and return a new pbq instance
func (m *Manager) GetPBQ(partitionID string, createIfMissing bool, storeType string) (*PBQ, bool, error) {

	pbqInstance, ok := m.pbqMap[partitionID]
	if ok {
		return pbqInstance, false, nil
	}

	// no match and nothing to create
	if !ok && !createIfMissing {
		return nil, false, NonExistentPBQError
	}

	// no match and create if missing
	var persistentStore store.Store
	var err error

	switch storeType {
	case dfv1.InMemoryStoreType:
		persistentStore, err = memory.NewMemoryStore(m.options)
		if err != nil {
			return nil, true, err
		}
	case dfv1.FileSystemStoreType:
		// TODO return a NotImplementedError
		return nil, true, nil
	}
	pbq, err := NewPBQ(partitionID, persistentStore, m.options)
	if err != nil {
		return nil, true, err
	}
	m.pbqMap[partitionID] = pbq
	return pbq, true, nil
}

// StartUp creates list of PBQ instances using the persistent store
// TODO
func (m *Manager) StartUp(ctx context.Context) {
	// storeoptions -> update pbqmap
	// setting the replay flag.
	// even if its exchanging pointers
	return
}

//ShutDown for clean shut down, flushes pending messages to store and closes the store
func (m *Manager) ShutDown(ctx context.Context) error {
	// iterate through the map of pbq
	// close all the pbq
	for _, v := range m.pbqMap {
		err := v.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Register - intended to be used by PBQ to register itself with the manager.
func (m *Manager) Register() error {
	// TODO implement me
	return nil
}

// Deregister - intended to be used by PBQ to deregister itself after GC is called.
func (m *Manager) Deregister() error {
	// TODO implement me
	return nil
}
