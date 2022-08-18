package pbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

type PBQ struct {
	Store  store.Store
	output chan *isb.Message
	input  chan *isb.Message
}

//NewPBQ accepts size and store and returns new PBQ
func NewPBQ(ctx context.Context, size int, store store.Store) (*PBQ, error) {

	// input channel is not buffered and output channel is buffered
	p := &PBQ{
		Store:  store,
		input:  make(chan *isb.Message),
		output: make(chan *isb.Message, size),
	}

	go func() {
		p.tee(ctx)
		//done writing to output channel and store's write channel close them
		close(p.output)
		p.Store.CloseCh()
	}()

	return p, nil
}

// reliable ack handling

//tee forwards input message to output channel and store's write channel
func (p *PBQ) tee(ctx context.Context) {
ioLoop:
	for elem := range p.input {
		select {
		case p.output <- elem:
		case <-ctx.Done():
			break
		}

		select {
		case p.Store.WriterCh() <- elem:
		case <-ctx.Done():
			break ioLoop
		}
	}
}

//WriteFromISB exposes write channel to write to PBQ
func (p *PBQ) WriteFromISB() chan<- *isb.Message {
	return p.input
}

//CloseOfBook closes input channel
func (p *PBQ) CloseOfBook() {
	close(p.input)
}

//ReadFromPBQ exposes read channel to read message
func (p *PBQ) ReadFromPBQ() <-chan *isb.Message {
	return p.output
}

func (p *PBQ) Close() error {
	//TODO implement me
	return nil
}

func (p *PBQ) GC() error {
	//TODO implement me
	return nil
}
