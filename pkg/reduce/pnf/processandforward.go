package pnf

import (
	"context"
	"errors"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"math"
	"strconv"
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
	Key              partition.ID
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
		Key:              partitionID,
		UDF:              udf,
		pbqReader:        pbqReader,
		log:              logging.FromContext(ctx),
		toBuffers:        toBuffers,
		whereToDecider:   whereToDecider,
		publishWatermark: pw,
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
	// example 123-456-key-hl -> key-hl
	strArr := strings.SplitAfterN(p.Key.String(), "-", 3)
	messageKey := strArr[2]

	// extract window end time from the partitionID, which will be used for watermark
	winEndTimestamp, _ := strconv.ParseInt(strArr[1], 0, 64)
	processorWM := processor.Watermark(time.Unix(winEndTimestamp, 0))

	to, err := p.whereToDecider.WhereTo(messageKey)
	if err != nil {
		return err
	}
	messagesToStep := p.whereToStep(to)

	//store write offsets to publish watermark
	writeOffsets := make(map[string][]isb.Offset)
	// write to isb
	// parallel writes to isb
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
			offsets, writeErr := p.writeToBuffer(ctx, bufferID, resultMessages)
			if writeErr != nil {
				success = false
				return
			}
			mu.Lock()
			writeOffsets[bufferID] = offsets
			mu.Unlock()
		}()
	}

	// wait until all the writer go routines return
	wg.Wait()
	// even if one write go routines fails, don't ack just return
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

func (p *ProcessAndForward) writeToBuffer(ctx context.Context, bufferID string, resultMessages []isb.Message) ([]isb.Offset, error) {
	var ISBWriteBackoff = wait.Backoff{
		Steps:    math.MaxInt64,
		Duration: 1 * time.Second,
		Factor:   1.5,
		Jitter:   0.1,
	}

	// write to isb with infinite exponential backoff (till shutdown is triggered)
	var failedMessages []isb.Message
	var offsets []isb.Offset
	writeErr := wait.ExponentialBackoffWithContext(ctx, ISBWriteBackoff, func() (done bool, err error) {
		var writeErrs []error
		offsets, writeErrs = p.toBuffers[bufferID].Write(ctx, resultMessages)
		for i, message := range resultMessages {
			if writeErrs[i] != nil {
				failedMessages = append(failedMessages, message)
			}
		}
		// retry only the failed messages
		if len(failedMessages) > 0 {
			resultMessages = failedMessages
			return false, nil
		}
		return true, nil
	})
	return offsets, writeErr
}

func (p *ProcessAndForward) publishWM(wm processor.Watermark, writeOffsets map[string][]isb.Offset) {
	for bufferName, offsets := range writeOffsets {
		if publisher, ok := p.publishWatermark[bufferName]; ok {
			if len(offsets) > 0 {
				publisher.PublishWatermark(wm, offsets[len(offsets)-1])
			}
		}
	}
}
