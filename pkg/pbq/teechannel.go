package pbq

type Tee struct {
	Input   chan interface{}
	Outputs []*chan interface{}
}

func (t *Tee) tee() {
	for {
		v, open := <-t.Input
		if open {
			for _, och := range t.Outputs {
				println(len(*och))
				println("dequed and wrote to the output channel")
				*och <- v
			}
		} else {
			return
		}
	}
}

func (t *Tee) close() {
	close(t.Input)
}
