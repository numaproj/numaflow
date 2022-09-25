package pf

import (
	"context"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
	"google.golang.org/grpc/metadata"
	"sync"
)

// ProcessAndForward reads messages from pbq and invokes udf using grpc
// and forwards the results to ISB
type ProcessAndForward struct {
	key string
	UDF udfreducer.Reducer
}

// NewProcessAndForward will return a new ProcessAndForward instance
func NewProcessAndForward(key string, udf udfreducer.Reducer) *ProcessAndForward {
	return &ProcessAndForward{
		key: key,
		UDF: udf,
	}
}

// Process
// 1. read messages from pbq
// 2. invoke udf using grpc (client streaming)
// 3. wait and return the result
func (p *ProcessAndForward) Process(ctx context.Context, pbqReader pbq.Reader) ([]*isb.Message, error) {

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
		return nil, err
	}

	// delete the persisted messages from pbq
	err = pbqReader.GC()
	// if err != nil retry with backoff
	return reducedResult, nil
}
