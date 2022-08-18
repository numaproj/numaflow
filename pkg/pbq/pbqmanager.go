package pbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/pbq/store"
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
// it returns new PBQ
func (m *Manager) GetPBQ(partitionID string, createIfMissing bool, storeType string, opts ...store.PbQStoreOption) (p *PBQ, isNew bool) {
	if !createIfMissing {
		pbqInstance, ok := m.pbqMap[partitionID]
		if !ok {
			return nil, false
		}
		return pbqInstance, false
	}

	return
}

//StartUp creates list of PBQ instances using the persistent store
func (m *Manager) StartUp(ctx context.Context) {
	return
}

//ShutDown for clean shut down, flushes pending messages to store and closes the store
func (m *Manager) ShutDown(ctx context.Context) {

}
