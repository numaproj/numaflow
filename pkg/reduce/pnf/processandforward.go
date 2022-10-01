package pnf

import (
	"context"
	"errors"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"math"
	"strings"
	"sync"
	"time"

	functionsdk "github.com/numaproj/numaflow-go/pkg/function"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
	"google.golang.org/grpc/metadata"
)

// ProcessAndForward reads messages from pbq and invokes udf using grpc
// and forwards the results to ISB
type ProcessAndForward struct {
	Key            partition.ID
	UDF            udfreducer.Reducer
	result         []*isb.Message
	pbqReader      pbq.Reader
	log            *zap.SugaredLogger
	toBuffers      map[string]isb.BufferWriter
	whereToDecider forward.ToWhichStepDecider
}

// NewProcessAndForward will return a new ProcessAndForward instance
func NewProcessAndForward(ctx context.Context,
	partitionID partition.ID,
	udf udfreducer.Reducer,
	pbqReader pbq.Reader,
	toBuffers map[string]isb.BufferWriter,
	whereToDecider forward.ToWhichStepDecider) *ProcessAndForward {
	return &ProcessAndForward{
		Key:            partitionID,
		UDF:            udf,
		pbqReader:      pbqReader,
		log:            logging.FromContext(ctx),
		toBuffers:      toBuffers,
		whereToDecider: whereToDecider,
	}
}

// Process method reads messages from the supplied pbqReader, invokes UDF to reduce the result
// and finally indicates the
func (p *ProcessAndForward) Process(ctx context.Context) error {

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
func (p *ProcessAndForward) Forward(ctx context.Context) error {
	// splits the partitionId to get the message key
	// example 123-456-temp-hl -> temp-hl
	messageKey := strings.SplitAfterN(p.Key.String(), "-", 3)[2]
	to, err := p.whereToDecider.WhereTo(messageKey)

	if err != nil {
		return err
	}

	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][]isb.Message)
	writeMessages := make([]isb.Message, len(p.result))
	for idx, msg := range p.result {
		writeMessages[idx] = *msg
	}

	// if a message is mapped to a isb, all the messages will be mapped to same isb (key is same)
	switch {
	case sharedutil.StringSliceContains(to, dfv1.MessageKeyAll):
		for bufferID := range p.toBuffers {
			messagesToStep[bufferID] = writeMessages
		}
	case sharedutil.StringSliceContains(to, dfv1.MessageKeyAll):
	default:
		for _, bufferID := range to {
			messagesToStep[bufferID] = writeMessages
		}

	}

	// keep retrying until shutdown is triggered
	var ISBWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt64,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
	}

	// write to isb
	// parallel writes to isb
	var wg sync.WaitGroup
	success := true
	for key, messages := range messagesToStep {
		bufferID := key
		if messages == nil || len(messages) == 0 {
			continue
		}
		wg.Add(1)
		resultMessages := messages
		go func() {
			defer wg.Done()
			var failedMessages []isb.Message
			writeErr := wait.ExponentialBackoffWithContext(ctx, ISBWriteBackoff, func() (done bool, err error) {
				_, writeErrs := p.toBuffers[bufferID].Write(ctx, resultMessages)
				for i, message := range resultMessages {
					if writeErrs[i] != nil {
						failedMessages = append(failedMessages, message)
					}
				}
				if len(failedMessages) > 0 {
					resultMessages = failedMessages
					return false, nil
				}
				return true, nil
			})
			if writeErr != nil {
				success = false
			}
		}()
	}
	// even if one write go routines fails, don't ack just return
	if success == false {
		return errors.New("failed to forward the messages to isb")
	}
	// wait until all the writer go routines return
	wg.Wait()
	// TODO publish watermark using offsets
	// delete the persisted messages
	err = p.pbqReader.GC()
	if err != nil {
		return err
	}
	return nil
}
