package pbq

import (
	"github.com/numaproj/numaflow/pkg/pbqstore"
)

type PBQ struct {
	Store  pbqstore.Store
	Output chan interface{}
	tch    Tee
}

// WriteFromISB
func (P PBQ) WriteFromISB() chan<- interface{} {
	return P.tch.Input
}

func (P PBQ) CloseOfBook() {
	P.tch.close()
}

func (P PBQ) ReadFromPBQ() <-chan interface{} {
	return P.Output
}

func (P PBQ) Close() error {
	//TODO implement me
	panic("implement me")
}

func (P PBQ) GC() error {
	//TODO implement me
	panic("implement me")
}

func NewPBQ(store pbqstore.Store) *PBQ {
	output := make(chan interface{})
	tch := Tee{
		Input:   make(chan interface{}),
		Outputs: []chan interface{}{output, store.WriterCh()},
	}

	go tch.tee()

	p := &PBQ{
		Store:  store,
		Output: output,
		tch:    tch,
	}

	return p
}
