package pf

import (
	"context"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/pbq"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
	"google.golang.org/grpc/metadata"
	"sync"
)

// ProcessAndForward reads messages from pbq and invokes udf using grpc
// and forwards the results to ISB
type ProcessAndForward struct {
	key       string
	FSD       forward.ToWhichStepDecider
	toBuffers map[string]isb.BufferWriter
	UDF       udfreducer.Reducer
}

// NewProcessAndForward will return a new ProcessAndForward instance
func NewProcessAndForward(ctx context.Context, key string, fsd forward.ToWhichStepDecider, toBuffers map[string]isb.BufferWriter, udf udfreducer.Reducer) *ProcessAndForward {
	return &ProcessAndForward{
		key:       key,
		FSD:       fsd,
		toBuffers: toBuffers,
		UDF:       udf,
	}
}

// 1. read messages from pbq
// 2. invoke udf using grpc (client streaming)
// 3. get the result and write to ISB
func (p *ProcessAndForward) process(ctx context.Context, pbqReader pbq.Reader) {
	// TODO get the stream for reduce
	var wg sync.WaitGroup
	var reducedResult []*isb.Message
	var err error

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{functionsdk.DatumKey: p.key}))
		reducedResult, err = p.UDF.Reduce(ctx, pbqReader.ReadCh())
	}()

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
