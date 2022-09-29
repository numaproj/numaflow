package pandf

import (
	"context"
	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
	"google.golang.org/grpc/metadata"
	"sync"
)

// ProcessAndForward reads messages from pbq and invokes udf using grpc
// and forwards the results to ISB
type ProcessAndForward struct {
	Key       partition.ID
	UDF       udfreducer.Reducer
	result    []*isb.Message
	pbqReader pbq.Reader
}

// NewProcessAndForward will return a new ProcessAndForward instance
func NewProcessAndForward(partitionID partition.ID, udf udfreducer.Reducer, pbqReader pbq.Reader) *ProcessAndForward {
	return &ProcessAndForward{
		Key:       partitionID,
		UDF:       udf,
		pbqReader: pbqReader,
	}
}

// Process method reads messages from the supplied pbqReader, invokes UDF to reduce the result
// and finally indicates the
func (p *ProcessAndForward) Process(ctx context.Context) error {
	// TODO get the stream for reduce
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{functionsdk.DatumKey: p.Key.String()}))
		p.result, err = p.UDF.Reduce(ctx, p.pbqReader.ReadCh())
	}()

	// wait for the reduce method to return
	wg.Wait()
	return err
}

// Forward writes messages to ToBuffers
func (p *ProcessAndForward) Forward() error {
	// TODO write to buffer

	// delete the persisted messages from pbq
	return p.pbqReader.GC()
}
