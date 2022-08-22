package pbq

import (
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

type PBQ struct {
	Store  store.Store
	output chan *isb.Message
	closed bool
}

//NewPBQ accepts size and store and returns new PBQ
func NewPBQ(bufferSize int64, store store.Store) (*PBQ, error) {

	// output channel is buffered
	p := &PBQ{
		Store:  store,
		output: make(chan *isb.Message, bufferSize),
		closed: false,
	}

	return p, nil
}

//WriteFromISB writes message to pbq and persistent store
func (p *PBQ) WriteFromISB(message *isb.Message) error {
	if p.closed {
		return errors.New("pbq is closed, cannot write the message")
	}
	p.output <- message
	err := p.Store.WriteToStore(message)
	if err != nil {
		return err
	}
	return nil
}

//CloseOfBook closes output channel
func (p *PBQ) CloseOfBook() error {
	p.closed = true
	close(p.output)
	return nil
}

//ReadFromPBQ exposes read channel to read message
func (p *PBQ) ReadFromPBQ() <-chan *isb.Message {
	return p.output
}

func (p *PBQ) Close() error {
	err := p.Store.Close()
	if err != nil {
		return err
	}
	return nil
}

func (p *PBQ) GC() error {
	err := p.Store.GC()
	if err != nil {
		return err
	}
	return nil
}
