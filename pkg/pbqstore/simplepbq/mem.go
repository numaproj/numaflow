package simplepbq

import "context"

// MemoryStore implements PBQStore which stores the data in memory
type MemoryStore struct {
	input    chan interface{}
	store    []interface{}
	capacity int64
}

func NewMemoryStore() *MemoryStore {
	return nil
}

// Read reads data from the in memory storage
func (m MemoryStore) Read(ctx context.Context) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

// Write writes the data to the in memory storage
func (m MemoryStore) Write(data interface{}, ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}
