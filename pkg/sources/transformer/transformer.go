package transformer

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
)

type Impl struct {
	transformer applier.MapApplier
	logger      *zap.SugaredLogger
}

// tracker tracks an execution of a data transformation.
type tracker struct {
	// readMessage is the message passed to the transformer.
	readMessage *isb.ReadMessage
	// transformedMessages are list of messages returned by transformer.
	transformedMessages []*isb.ReadMessage
	// transformError is the error thrown by transformer.
	transformError error
}

func New(
	transformer applier.MapApplier,
	logger *zap.SugaredLogger,
) *Impl {
	i := &Impl{
		transformer: transformer,
		logger:      logger,
	}
	return i
}

func (h *Impl) Transform(ctx context.Context, messages []*isb.ReadMessage) []*isb.ReadMessage {
	var rms []*isb.ReadMessage
	// transformer concurrent processing request channel
	transformCh := make(chan *tracker)
	// transformTrackers stores the results after transformer finishes processing for all read messages.
	transformTrackers := make([]tracker, len(messages))

	var wg sync.WaitGroup
	// concurrently apply transformer to each of the messages.
	for i := 0; i < len(messages); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.concurrentApplyTransformer(ctx, transformCh)
		}()
	}

	concurrentProcessingStart := time.Now()
	// send transformer processing work to the channel
	for idx, readMessage := range messages {
		transformTrackers[idx].readMessage = readMessage
		transformCh <- &transformTrackers[idx]
	}
	// let the go routines know that there is no more work
	close(transformCh)
	// wait till the processing is done. this will not be an infinite wait because the transformer processing will exit if ctx.Done() is closed.
	wg.Wait()
	h.logger.Debugw("concurrent applyTransformer completed", zap.Int("concurrency", 100), zap.Duration("took", time.Since(concurrentProcessingStart)))

	// TODO - emit processing time metrics.

	// transformer processing is done, construct return list.
	for _, m := range transformTrackers {
		// check for errors, if one of the messages failed being transformed, stop processing the entire batch and return empty slice.
		if m.transformError != nil {
			// TODO - emit error metrics.
			h.logger.Errorw("failed to applyTransformer", zap.Error(m.transformError))
			return []*isb.ReadMessage{}
		}
		rms = append(rms, m.transformedMessages...)
	}
	return rms
}

// concurrentApplyTransformer applies the transformer based on the request from the channel
func (h *Impl) concurrentApplyTransformer(ctx context.Context, tracker <-chan *tracker) {
	for t := range tracker {
		// TODO - emit metrics.
		transformedMessages, err := h.applyTransformer(ctx, t.readMessage)
		t.transformedMessages = append(t.transformedMessages, transformedMessages...)
		t.transformError = err
	}
}

func (h *Impl) applyTransformer(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.ReadMessage, error) {
	var transformedMessages []*isb.ReadMessage
	for {
		select {
		case <-ctx.Done():
			return []*isb.ReadMessage{}, ctx.Err()
		default:
			msgs, err := h.transformer.ApplyMap(ctx, readMessage)
			if err != nil {
				h.logger.Errorw("Transformer.Apply error", zap.Error(err))
				// we don't expect user defined transformer returns error. keep retrying.
				// TODO - make retry interval configurable.
				time.Sleep(time.Millisecond)
				continue
			} else {
				for _, m := range msgs {
					if m.EventTime.IsZero() {
						m.EventTime = readMessage.EventTime
					}

					// construct isb.ReadMessage from isb.Message by providing ReadOffset and Watermark.

					// we inherit ReadOffset from parent ReadMessage.
					// this will cause multiple ReadMessages sharing exact same ReadOffset.
					// we need to be careful about such case, because some sources might not support acknowledging same ReadOffset more than once.
					// to mitigate such case, in data forwarder, we implement exact once ack for each distinct ReadOffset.

					// we also inherit Watermark from parent, which is ok because
					// after source data transformation, sourcer will publish new watermarks to fromBuffer and
					// data forwarder will fetch new watermarks and override the old ones for each of the ReadMessages.
					transformedMessages = append(transformedMessages, m.ToReadMessage(readMessage.ReadOffset, readMessage.Watermark))
				}
				return transformedMessages, nil
			}
		}
	}
}
