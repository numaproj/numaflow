package pf

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/pbq"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
	"sync"
)

// ProcessAndForward reads messages from pbq and invokes udf using grpc
// and forwards the results to ISB
type ProcessAndForward struct {
	FSD       forward.ToWhichStepDecider
	toBuffers map[string]isb.BufferWriter
	UDF       udfreducer.Reducer
}

// 1. read messages from pbq
// 2. invoke udf using grpc (client streaming)
// 3. get the result and write to ISB
func (p *ProcessAndForward) process(ctx context.Context, pbqReader pbq.Reader) {
	// TODO get the stream for reduce
	var wg sync.WaitGroup
	var reducedResult []*isb.Message
	var err error

	messageCh := make(chan *isb.Message)
	wg.Add(1)
	go func() {
		defer wg.Done()
		reducedResult, err = p.UDF.Reduce(ctx, messageCh)
	}()

	readCh := pbqReader.ReadCh()
readLoop:
	for {
		select {
		case msg, ok := <-readCh:
			if msg != nil {
				// select to avoid blocking write on messageCh
				select {
				case messageCh <- msg:
				case <-ctx.Done():
					return
				}
			}
			if !ok {
				break readLoop
			}
		case <-ctx.Done():
			return
		}
	}
	// close the message channel, let the reducer know they are no more messages
	close(messageCh)
	// wait for the reduce method to return
	wg.Wait()
	if err != nil {
		// TODO write result to ToBuffers
		return
	}

	// write result to buffers
	err = p.writeToBuffers(reducedResult)
	if err != nil {
		return
	}

	// delete the persisted messages from pbq
	err = pbqReader.GC()
	if err != nil {
		return
	}

}

// write messages to ToBuffers
func (p *ProcessAndForward) writeToBuffers(messages []*isb.Message) error {
	// TODO write to buffer
	return nil
}
