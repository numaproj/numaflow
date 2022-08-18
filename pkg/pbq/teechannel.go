package pbq

type Tee struct {
	Input   chan interface{}
	Outputs []chan interface{}
}

func (t *Tee) close() {
	close(t.Input)
}
