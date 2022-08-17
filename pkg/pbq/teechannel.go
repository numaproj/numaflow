package pbq

type Tee struct {
	Input   chan interface{}
	Outputs []chan interface{}
}

func (t *Tee) tee() {
	for {
		v, open := <-t.Input
		if open {
			for i := 0; i < len(t.Outputs); i++ {
				t.Outputs[i] <- v
			}
		} else {
			return
		}
	}
}

func (t *Tee) close() {
	close(t.Input)
}
