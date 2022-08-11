package simplepbq

// PBQ implements a simple PBQ with in memory persistence.
//type PBQ struct {
//	Storage     chan [][]byte
//	Capacity    int64
//	PartitionId string
//}

type MemoryStore struct {
	Storage     chan [][]byte
	Capacity    int64
	PartitionId string
}

func NewStore() *MemoryStore {
	return &MemoryStore{}
}

func (ms *MemoryStore) Read(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (ms *MemoryStore) Write(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (ms *MemoryStore) Close() error {
	//TODO implement me
	panic("implement me")
}
