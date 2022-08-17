package pbqstore

// Store provides methods to read, write and delete data from a durable store.
type Store interface {
	// Read is used to read the data from the Store
	ReadCh() chan interface{}
	// Write writes the data to the store
	WriterCh() chan interface{}
}
