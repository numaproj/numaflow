package pbq

import (
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

type PBQ struct {
	Store       store.Store
	output      chan *isb.Message
	closed      bool // closed to avoid panic in case writes happen after closeofbook
	partitionID string
}

//NewPBQ accepts size and store and returns new PBQ
func NewPBQ(partitionID string, bufferSize int64, store store.Store) (*PBQ, error) {

	// output channel is buffered
	p := &PBQ{
		Store:       store,
		output:      make(chan *isb.Message, bufferSize),
		closed:      false,
		partitionID: partitionID,
	}

	return p, nil
}

// WriteFromISB writes message to pbq and persistent store
// We don't need a context here as this is invoked for every message.
func (p *PBQ) WriteFromISB(message *isb.Message) (WriteErr error) {
	if p.closed {
		return errors.New("pbq is closed, cannot write the message")
	}
	p.output <- message
	WriteErr = p.Store.WriteToStore(message)
	return
}

//CloseOfBook closes output channel
func (p *PBQ) CloseOfBook() {
	p.closed = true
	close(p.output)
}

// if one partition created back pressure it will affect all the other partitions

//ReadFromPBQ exposes read channel to read message
func (p *PBQ) ReadFromPBQ() <-chan *isb.Message {
	return p.output
}

// Close is used by the Reader to indicate that it has finished
// consuming the data from output channel
func (p *PBQ) Close() (CloseErr error) {
	CloseErr = p.Store.Close()
	return
}

// GC is invoked after the Reader (ProcessAndForward) has finished
// Forwarding the output to ISB.
func (p *PBQ) GC() (GcErr error) {
	GcErr = p.Store.GC()
	p.Store = nil
	return
}
