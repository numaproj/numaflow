package pbq

type Tee struct {
	Input   chan interface{}
	Outputs []chan interface{}
}

func (t *Tee) tee() {
	for elem := range t.Input {
		for _, och := range t.Outputs {
			och <- elem
		}
	}
}

func (t *Tee) close() {
	close(t.Input)
}
