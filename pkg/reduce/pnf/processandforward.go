// Package pnf processes and then forwards messages belonging to a window. It reads the data from PBQ (which is populated
// by the `readloop`), calls the UDF reduce function, and then forwards to the next ISB. After a successful forward, it
// invokes `GC` to clean up the PBQ. Since pnf is a reducer, it mutates the watermark. The watermark after the pnf will
// be the end time of the window.
package pnf

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	udfreducer "github.com/numaproj/numaflow/pkg/udf/reducer"
)

// ProcessAndForward reads messages from pbq, invokes udf using grpc, forwards the results to ISB, and then publishes
// the watermark for that partition.
type ProcessAndForward struct {
	PartitionID      partition.ID
	UDF              udfreducer.Reducer
	result           []*isb.Message
	pbqReader        pbq.Reader
	log              *zap.SugaredLogger
	toBuffers        map[string]isb.BufferWriter
	whereToDecider   forward.ToWhichStepDecider
	publishWatermark map[string]publish.Publisher
}

// NewProcessAndForward will return a new ProcessAndForward instance
func NewProcessAndForward(ctx context.Context,
	partitionID partition.ID,
	udf udfreducer.Reducer,
	pbqReader pbq.Reader,
	toBuffers map[string]isb.BufferWriter,
	whereToDecider forward.ToWhichStepDecider, pw map[string]publish.Publisher) *ProcessAndForward {
	return &ProcessAndForward{
		PartitionID:      partitionID,
		UDF:              udf,
		pbqReader:        pbqReader,
		log:              logging.FromContext(ctx),
		toBuffers:        toBuffers,
		whereToDecider:   whereToDecider,
		publishWatermark: pw,
	}
}

// Process method reads messages from the supplied PBQ, invokes UDF to reduce the result.
func (p *ProcessAndForward) Process(ctx context.Context) error {
	var err error

	// FIXME: we need to fix https://github.com/numaproj/numaflow-go/blob/main/pkg/function/service.go#L101
	// blocking call, only returns the result after it has read all the messages from pbq
	p.result, err = p.UDF.Reduce(ctx, &p.PartitionID, p.pbqReader.ReadCh())
	return err
}

// Forward writes messages to the ISBs, publishes watermark, and invokes GC on PBQ.
func (p *ProcessAndForward) Forward(ctx context.Context) error {
	// extract window end time from the partitionID, which will be used for watermark
	processorWM := processor.Watermark(p.PartitionID.End)

	// decide which ISB to write to
	to, err := p.whereToDecider.WhereTo(p.PartitionID.Key)
	if err != nil {
		return err
	}
	messagesToStep := p.whereToStep(to)

	// store write offsets to publish watermark
	writeOffsets := make(map[string][]isb.Offset)

	// parallel writes to each isb
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := true
	for key, messages := range messagesToStep {
		bufferID := key
		if len(messages) == 0 {
			continue
		}
		wg.Add(1)
		resultMessages := messages
		go func() {
			defer wg.Done()
			offsets, ctxClosedErr := p.writeToBuffer(ctx, bufferID, resultMessages)
			if ctxClosedErr != nil {
				success = false
				p.log.Errorw("Context closed while waiting to write the message to ISB", zap.Error(ctxClosedErr), zap.Any("partitionID", p.PartitionID))
				return
			}
			mu.Lock()
			// TODO: do we need lock? isn't each buffer isolated since we do sequential per ISB?
			writeOffsets[bufferID] = offsets
			mu.Unlock()
		}()
	}

	// wait until all the writer go routines return
	wg.Wait()
	// even if one write go routines fails, don't GC just return
	if !success {
		return errors.New("failed to forward the messages to isb")
	}

	p.publishWM(processorWM, writeOffsets)
	// delete the persisted messages
	err = p.pbqReader.GC()
	if err != nil {
		return err
	}
	return nil
}

// whereToStep assigns a message to the ISBs based on the Message.Key.
// TODO: we have to introduce support for shuffle, output of a reducer can be input to the next reducer.
func (p *ProcessAndForward) whereToStep(to []string) map[string][]isb.Message {
	// writer doesn't accept array of pointers
	messagesToStep := make(map[string][]isb.Message)
	writeMessages := make([]isb.Message, len(p.result))
	for idx, msg := range p.result {
		writeMessages[idx] = *msg
	}

	// if a message is mapped to an isb, all the messages will be mapped to same isb (key is same)
	switch {
	case sharedutil.StringSliceContains(to, dfv1.MessageKeyAll):
		for bufferID := range p.toBuffers {
			messagesToStep[bufferID] = writeMessages
		}
	case sharedutil.StringSliceContains(to, dfv1.MessageKeyDrop):
	default:
		for _, bufferID := range to {
			messagesToStep[bufferID] = writeMessages
		}

	}
	return messagesToStep
}

// writeToBuffer writes to the ISBs.
// TODO: is there any point in returning an error here? this is an infinite loop and the only error is ctx.Done!
func (p *ProcessAndForward) writeToBuffer(ctx context.Context, bufferID string, resultMessages []isb.Message) ([]isb.Offset, error) {
	var ISBWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt,
		Duration: 100 * time.Millisecond,
		Factor:   1,
		Jitter:   0.1,
	}

	writeMessages := resultMessages

	// write to isb with infinite exponential backoff (until shutdown is triggered)
	var offsets []isb.Offset
	ctxClosedErr := wait.ExponentialBackoffWithContext(ctx, ISBWriteBackoff, func() (done bool, err error) {
		var writeErrs []error
		var failedMessages []isb.Message
		offsets, writeErrs = p.toBuffers[bufferID].Write(ctx, writeMessages)
		for i, message := range writeMessages {
			if writeErrs[i] != nil {
				failedMessages = append(failedMessages, message)
			}
		}
		// retry only the failed messages
		if len(failedMessages) > 0 {
			p.log.Warnw("Failed to write messages to isb inside pnf", zap.Errors("errors", writeErrs))
			writeMessages = failedMessages
			return false, nil
		}
		return true, nil
	})
	return offsets, ctxClosedErr
}

// publishWM publishes the watermark to each edge.
func (p *ProcessAndForward) publishWM(wm processor.Watermark, writeOffsets map[string][]isb.Offset) {
	for bufferName, offsets := range writeOffsets {
		if publisher, ok := p.publishWatermark[bufferName]; ok {
			if len(offsets) > 0 {
				publisher.PublishWatermark(wm, offsets[len(offsets)-1])
			}
		}
	}
}
