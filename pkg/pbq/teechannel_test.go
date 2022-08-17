package pbq

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestBaseCase(t *testing.T) {

	l := 10
	o1 := make(chan interface{}, l)
	o2 := make(chan interface{}, l)
	outputs := []chan interface{}{o1, o2}
	tc := &Tee{
		Input:   make(chan interface{}),
		Outputs: outputs,
	}
	go tc.tee()

	for i := 0; i < l; i++ {
		tc.Input <- i
		v1 := <-o1
		assert.Equal(t, i, v1.(int))
		v2 := <-o2
		assert.Equal(t, i, v2.(int))
	}

}

func TestOutputWithClosedInput(t *testing.T) {
	l := 10
	o1 := make(chan interface{}, l)
	o2 := make(chan interface{}, l)
	outputs := []chan interface{}{o1, o2}
	tc := &Tee{
		Input:   make(chan interface{}),
		Outputs: outputs,
	}
	go tc.tee()

	for i := 0; i < l; i++ {
		tc.Input <- i
	}

	tc.close()

	assert.Len(t, o1, l)
	assert.Len(t, o2, l)

}

func TestBackPressureHandling(t *testing.T) {
	l := 10
	c := 2

	o1 := make(chan interface{}, l)
	o2 := make(chan interface{}, c)
	outputs := []chan interface{}{o1, o2}
	tc := &Tee{
		Input:   make(chan interface{}),
		Outputs: outputs,
	}

	go tc.tee()

	var wg sync.WaitGroup

	go func(tc *Tee) {
		wg.Add(1)
		defer wg.Done()
		timer := time.NewTimer(2 * time.Second)
		for i := 0; i < l; i++ {
			select {
			case <-timer.C:
				tc.close()
				return
			case tc.Input <- i:
				t.Log("wrote to the channel")
			}
		}
	}(tc)

	wg.Wait()

}
