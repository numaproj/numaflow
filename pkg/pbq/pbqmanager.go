package pbq

import (
	"context"
	"errors"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/store/memory"
)

var NonExistentPBQError error = errors.New("missing PBQ for the partition")

// Manager helps in managing the lifecycle of PBQ instances
type Manager struct {
	// options
	pbqMap map[string]*PBQ
	// pbqstore options
}

//NewManager returns new instance of manager
func NewManager(opts ...store.StoreOptions) *Manager {
	manager := &Manager{pbqMap: make(map[string]*PBQ)}
	return manager
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

//GetPBQ returns pbq for the given partitionID, if createIfMissing is set to true
// it creates and return a new pbq instance
func (m *Manager) GetPBQ(partitionID string, createIfMissing bool, bufferSize int64, storeType string, opts ...store.PbQStoreOption) (*PBQ, bool, error) {

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
		persistentStore, err = memory.NewMemoryStore(opts...)
		if err != nil {
			return nil, true, err
		}
	case dfv1.FileSystemStoreType:
		return nil, true, nil
	}
	pbq, err := NewPBQ(partitionID, bufferSize, persistentStore)
	if err != nil {
		return nil, true, err
	}
	m.pbqMap[partitionID] = pbq
	return pbq, true, nil
}

//StartUp creates list of PBQ instances using the persistent store
func (m *Manager) StartUp(ctx context.Context) {
	// storeoptions -> update pbqmap
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
