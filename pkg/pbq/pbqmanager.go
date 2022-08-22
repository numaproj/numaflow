package pbq

import (
	"context"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/store/memory"
)

// Manager helps in managing the lifecycle of PBQ instances
type Manager struct {
	// options
	pbqMap map[string]*PBQ
}

//NewManager returns new instance of manager
func NewManager(opts ...store.StoreOptions) *Manager {
	manager := &Manager{pbqMap: make(map[string]*PBQ)}
	return manager
}

// ListPartitions returns all the pbq instances
func (m *Manager) ListPartitions() []*PBQ {
	if len(m.pbqMap) == 0 {
		return nil
	}
	pbqList := make([]*PBQ, len(m.pbqMap))
	for _, val := range m.pbqMap {
		pbqList = append(pbqList, val)
	}
	return pbqList
}

//GetPBQ returns pbq for the given partitionID, if createIfMissing is set to true
// it creates and return a new pbq instance
func (m *Manager) GetPBQ(partitionID string, createIfMissing bool, bufferSize int64, storeType string, opts ...store.PbQStoreOption) (*PBQ, bool, error) {
	if !createIfMissing {
		pbqInstance, ok := m.pbqMap[partitionID]
		if !ok {
			return nil, false, nil
		}
		return pbqInstance, false, nil
	}

	var persistentStore store.Store
	var err error

	switch storeType {
	case dfv1.InMemoryStoreType:
		persistentStore, err = memory.NewMemoryStore(opts...)
		if err != nil {
			return nil, true, err
		}
	case dfv1.FileSystemStoreType:

	}
	pbq, err := NewPBQ(bufferSize, persistentStore)
	if err != nil {
		return nil, true, err
	}
	m.pbqMap[partitionID] = pbq
	return pbq, true, nil
}

//StartUp creates list of PBQ instances using the persistent store
func (m *Manager) StartUp(ctx context.Context) {
	return
}

//ShutDown for clean shut down, flushes pending messages to store and closes the store
func (m *Manager) ShutDown(ctx context.Context) {

}
