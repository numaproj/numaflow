package pbq

import "io"

// PBQStore provides methods to read, write and delete data from a durable store.
type PBQStore interface {
	io.ReadWriteCloser
}

// PBQReader provides methods to read from PBQ.
type PBQReader interface {
	io.Reader
}

// PBQCleaner deletes all the data from a PBQ. Deletion of data from PBQ is Complete.
// Once invoked, all the data will be deleted from a PBQ.
type PBQCleaner interface {
	Clean()
}

// PBQProcessor provides methods to read and delete data from a PBQ.
type PBQProcessor interface {
	PBQReader
	PBQCleaner
}

// PBQWriter provides methods to write data to and close a PBQ.
// No data can be written to PBQ after it is closed.
type PBQWriter interface {
	io.WriteCloser
}
