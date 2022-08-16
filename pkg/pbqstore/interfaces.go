package pbqstore

import "context"

// Store provides methods to read, write and delete data from a durable store.
type Store interface {
	// Read is used to read the data from the Store
	Read(ctx context.Context) (interface{}, error)
	// Write writes the data to the store
	Write(data interface{}, ctx context.Context) error
}
