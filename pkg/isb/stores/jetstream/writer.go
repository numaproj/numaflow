/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jetstream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

type jetStreamWriter struct {
	name         string
	partitionIdx int32
	stream       string
	subject      string
	client       *jsclient.Client
	js           nats.JetStreamContext
	opts         *writeOptions
	isFull       *atomic.Bool
	log          *zap.SugaredLogger
}

// NewJetStreamBufferWriter is used to provide a new instance of JetStreamBufferWriter
func NewJetStreamBufferWriter(ctx context.Context, client *jsclient.Client, name, stream, subject string, partitionIdx int32, opts ...WriteOption) (isb.BufferWriter, error) {
	o := defaultWriteOptions()
	for _, opt := range opts {
		if opt != nil {
			if err := opt(o); err != nil {
				return nil, err
			}
		}
	}

	js, err := client.JetStreamContext(nats.PublishAsyncMaxPending(1024))

	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	result := &jetStreamWriter{
		name:         name,
		partitionIdx: partitionIdx,
		stream:       stream,
		subject:      subject,
		client:       client,
		js:           js,
		opts:         o,
		isFull:       atomic.NewBool(true),
		log:          logging.FromContext(ctx).With("bufferWriter", name).With("stream", stream).With("subject", subject).With("partitionIdx", partitionIdx),
	}

	go result.runStatusChecker(ctx)
	return result, nil
}

func (jw *jetStreamWriter) runStatusChecker(ctx context.Context) {
	labels := map[string]string{"buffer": jw.GetName()}
	// Use a separated JetStream context for status checker
	js, err := jw.client.JetStreamContext()
	if err != nil {
		// Let it exit if it fails to start the status checker
		jw.log.Fatal("Failed to get Jet Stream context, %w", err)
	}
	checkStatus := func() {
		s, err := js.StreamInfo(jw.stream)
		if err != nil {
			isbFullErrors.With(labels).Inc()
			jw.log.Errorw("Failed to get stream info in the writer", zap.Error(err))
			return
		}
		c, err := js.ConsumerInfo(jw.stream, jw.stream)
		if err != nil {
			isbFullErrors.With(labels).Inc()
			jw.log.Errorw("Failed to get consumer info in the writer", zap.Error(err))
			return
		}
		var solidUsage, softUsage float64
		softUsage = (float64(c.NumPending) + float64(c.NumAckPending)) / float64(jw.opts.maxLength)
		if s.Config.Retention == nats.LimitsPolicy {
			solidUsage = softUsage
		} else {
			solidUsage = float64(s.State.Msgs) / float64(jw.opts.maxLength)
		}
		// TODO: solid usage calculation might be incorrect due to incorrect JetStream metadata issue caused by pod migration, need to revisit this later.
		// We should set up alerts with sort of rules to detect the metadata issue. For example, solidUsage is too much greater than softUsage for a while.
		if solidUsage >= jw.opts.bufferUsageLimit && softUsage >= jw.opts.bufferUsageLimit {
			jw.log.Infow("Usage is greater than bufferUsageLimit", zap.Any("solidUsage", solidUsage), zap.Any("softUsage", softUsage))
			jw.isFull.Store(true)
		} else {
			jw.isFull.Store(false)
		}
		isbSoftUsage.With(labels).Set(softUsage)
		isbSolidUsage.With(labels).Set(solidUsage)
		isbPending.With(labels).Set(float64(c.NumPending))
		isbAckPending.With(labels).Set(float64(c.NumAckPending))

		jw.log.Debugw("Consumption information", zap.Any("totalMsgs", s.State.Msgs), zap.Any("pending", c.NumPending),
			zap.Any("ackPending", c.NumAckPending), zap.Any("waiting", c.NumWaiting),
			zap.Any("ackFloorStreamId", c.AckFloor.Stream), zap.Any("deliveredStreamId", c.Delivered.Stream),
			zap.Float64("solidUsage", solidUsage), zap.Float64("softUsage", softUsage))
	}
	checkStatus()

	ticker := time.NewTicker(jw.opts.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			checkStatus()
		case <-ctx.Done():
			return
		}
	}
}

func (jw *jetStreamWriter) GetName() string {
	return jw.name
}

func (jw *jetStreamWriter) GetPartitionIdx() int32 {
	return jw.partitionIdx
}

// Close doesn't have to do anything for JetStreamBufferWriter, client will be closed by the caller.
func (jw *jetStreamWriter) Close() error {
	return nil
}

func (jw *jetStreamWriter) WriteNew(ctx context.Context, message isb.Message) (isb.Offset, error) {
	labels := map[string]string{"buffer": jw.GetName()}
	errs := fmt.Errorf("unknown error")
	if jw.isFull.Load() {
		jw.log.Debugw("Is full")
		isbFull.With(labels).Inc()
		// when buffer is full, we need to decide whether to discard the message or not.
		switch jw.opts.bufferFullWritingStrategy {
		case v1alpha1.DiscardLatest:
			// user explicitly wants to discard the message when buffer if full.
			// return no retryable error as a callback to let caller know that the message is discarded.
			errs = isb.NonRetryableBufferWriteErr{Name: jw.name, Message: isb.BufferFullMessage}
		default:
			// Default behavior is to return a BufferWriteErr.
			errs = isb.BufferWriteErr{Name: jw.name, Full: true, Message: isb.BufferFullMessage}
		}
		isbWriteErrors.With(labels).Inc()
		return nil, errs
	}
	//// TODO: temp env flag for sync/async writing, revisit this later.
	//if sharedutil.LookupEnvStringOr("ISB_ASYNC_WRITE", "false") == "true" {
	//	return jw.asyncWrite(ctx, messages, errs, labels)
	//}

	routine, err := jw.WriteRoutine(message, labels)
	return routine, err
}

func (jw *jetStreamWriter) Write(ctx context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	labels := map[string]string{"buffer": jw.GetName()}
	var errs = make([]error, len(messages))
	for i := 0; i < len(errs); i++ {
		errs[i] = fmt.Errorf("unknown error")
	}
	if jw.isFull.Load() {
		jw.log.Debugw("Is full")
		isbFull.With(map[string]string{"buffer": jw.GetName()}).Inc()
		// when buffer is full, we need to decide whether to discard the message or not.
		switch jw.opts.bufferFullWritingStrategy {
		case v1alpha1.DiscardLatest:
			// user explicitly wants to discard the message when buffer if full.
			// return no retryable error as a callback to let caller know that the message is discarded.
			for i := 0; i < len(errs); i++ {
				errs[i] = isb.NonRetryableBufferWriteErr{Name: jw.name, Message: isb.BufferFullMessage}
			}
		default:
			// Default behavior is to return a BufferWriteErr.
			for i := 0; i < len(errs); i++ {
				errs[i] = isb.BufferWriteErr{Name: jw.name, Full: true, Message: isb.BufferFullMessage}
			}
		}
		isbWriteErrors.With(labels).Inc()
		return nil, errs
	}
	// TODO: temp env flag for sync/async writing, revisit this later.
	if sharedutil.LookupEnvStringOr("ISB_ASYNC_WRITE", "false") == "true" {
		return jw.asyncWrite(ctx, messages, errs, labels)
	}
	return jw.syncWrite(ctx, messages, errs, labels)
}

func (jw *jetStreamWriter) asyncWrite(_ context.Context, messages []isb.Message, errs []error, metricsLabels map[string]string) ([]isb.Offset, []error) {
	var writeOffsets = make([]isb.Offset, len(messages))
	var futures = make([]nats.PubAckFuture, len(messages))
	for index, message := range messages {
		payload, err := message.MarshalBinary()
		if err != nil {
			errs[index] = err
			continue
		}
		m := &nats.Msg{
			Subject: jw.subject,
			Data:    payload,
		}
		var pubOpts []nats.PubOpt
		// nats.MsgId() is for exactly-once writing
		// we don't need to set MsgId for control message
		if message.Header.Kind != isb.WMB {
			pubOpts = append(pubOpts, nats.MsgId(message.Header.ID))
		}
		if future, err := jw.js.PublishMsgAsync(m, pubOpts...); err != nil { // nats.MsgId() is for exactly-once writing
			errs[index] = err
		} else {
			futures[index] = future
		}
	}
	futureCheckDone := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := new(sync.WaitGroup)
	for index, f := range futures {
		if f == nil {
			continue
		}
		wg.Add(1)
		go func(idx int, fu nats.PubAckFuture) {
			defer wg.Done()
			select {
			case pubAck := <-fu.Ok():
				if pubAck.Duplicate {
					// If a message gets repeated, it will have the same offset number as the one before it.
					// We shouldn't try to publish watermark on these repeated messages. Doing so would
					// violate the principle of publishing watermarks to monotonically increasing offsets.
					errs[idx] = isb.NonRetryableBufferWriteErr{Name: jw.name, Message: isb.DuplicateIDMessage}
				} else {
					writeOffsets[idx] = &writeOffset{seq: pubAck.Sequence, partitionIdx: jw.partitionIdx}
					errs[idx] = nil
					jw.log.Debugw("Succeeded to publish a message", zap.String("stream", pubAck.Stream), zap.Any("seq", pubAck.Sequence), zap.Bool("duplicate", pubAck.Duplicate), zap.String("domain", pubAck.Domain))
				}
			case err := <-fu.Err():
				errs[idx] = err
				isbWriteErrors.With(metricsLabels).Inc()
			case <-ctx.Done():
			}
		}(index, f)
	}

	go func() {
		wg.Wait()
		close(futureCheckDone)
	}()
	timeout := time.After(5 * time.Second)
	select {
	case <-futureCheckDone:
	case <-timeout:
		cancel()
		<-futureCheckDone
		// TODO: Maybe need to reconnect.
		isbWriteTimeout.With(metricsLabels).Inc()
	}
	return writeOffsets, errs
}

func (jw *jetStreamWriter) WriteRoutine(message isb.Message, metricsLabels map[string]string) (isb.Offset, error) {
	defer func(t time.Time) {
		isbWriteTime.With(metricsLabels).Observe(float64(time.Since(t).Microseconds()))
	}(time.Now())
	payload, err := message.MarshalBinary()
	if err != nil {
		return nil, err
	}
	m := &nats.Msg{
		Subject: jw.subject,
		Data:    payload,
	}
	pubOpts := []nats.PubOpt{nats.AckWait(2 * time.Second)}
	// nats.MsgId() is for exactly-once writing
	// we don't need to set MsgId for control message
	if message.Header.Kind != isb.WMB {
		pubOpts = append(pubOpts, nats.MsgId(message.Header.ID))
	}
	if pubAck, err := jw.js.PublishMsg(m, pubOpts...); err != nil {
		isbWriteErrors.With(metricsLabels).Inc()
		return nil, err
	} else {
		if pubAck.Duplicate {
			// If a message gets repeated, it will have the same offset number as the one before it.
			// We shouldn't try to publish watermark on these repeated messages. Doing so would
			// violate the principle of publishing watermarks to monotonically increasing offsets.
			return nil, isb.NonRetryableBufferWriteErr{Name: jw.name, Message: isb.DuplicateIDMessage}
		} else {
			jw.log.Debugw("Succeeded to publish a message", zap.String("stream", pubAck.Stream), zap.Any("seq", pubAck.Sequence), zap.Bool("duplicate", pubAck.Duplicate), zap.String("msgID", message.Header.ID), zap.String("domain", pubAck.Domain))
			return &writeOffset{seq: pubAck.Sequence, partitionIdx: jw.partitionIdx}, nil
		}
	}
}

func (jw *jetStreamWriter) syncWrite(_ context.Context, messages []isb.Message, errs []error, metricsLabels map[string]string) ([]isb.Offset, []error) {
	defer func(t time.Time) {
		isbWriteTime.With(metricsLabels).Observe(float64(time.Since(t).Microseconds()))
	}(time.Now())
	if len(messages) == 1 {
		routine, err := jw.WriteRoutine(messages[0], metricsLabels)
		return []isb.Offset{routine}, []error{err}
	}
	var writeOffsets = make([]isb.Offset, len(messages))
	wg := new(sync.WaitGroup)
	for index, msg := range messages {
		wg.Add(1)
		go func(message isb.Message, idx int) {
			defer wg.Done()
			payload, err := message.MarshalBinary()
			if err != nil {
				errs[idx] = err
				return
			}
			m := &nats.Msg{
				Subject: jw.subject,
				Data:    payload,
			}
			pubOpts := []nats.PubOpt{nats.AckWait(2 * time.Second)}
			// nats.MsgId() is for exactly-once writing
			// we don't need to set MsgId for control message
			if message.Header.Kind != isb.WMB {
				pubOpts = append(pubOpts, nats.MsgId(message.Header.ID))
			}
			if pubAck, err := jw.js.PublishMsg(m, pubOpts...); err != nil {
				errs[idx] = err
				isbWriteErrors.With(metricsLabels).Inc()
			} else {
				if pubAck.Duplicate {
					// If a message gets repeated, it will have the same offset number as the one before it.
					// We shouldn't try to publish watermark on these repeated messages. Doing so would
					// violate the principle of publishing watermarks to monotonically increasing offsets.
					errs[idx] = isb.NonRetryableBufferWriteErr{Name: jw.name, Message: isb.DuplicateIDMessage}
				} else {
					writeOffsets[idx] = &writeOffset{seq: pubAck.Sequence, partitionIdx: jw.partitionIdx}
					errs[idx] = nil
					jw.log.Debugw("Succeeded to publish a message", zap.String("stream", pubAck.Stream), zap.Any("seq", pubAck.Sequence), zap.Bool("duplicate", pubAck.Duplicate), zap.String("msgID", message.Header.ID), zap.String("domain", pubAck.Domain))
				}
			}
		}(msg, index)
	}
	wg.Wait()
	return writeOffsets, errs
}

// writeOffset is the offset of the location in the JS stream we wrote to.
type writeOffset struct {
	seq          uint64
	partitionIdx int32
}

func (w *writeOffset) String() string {
	return fmt.Sprint(w.seq)
}

func (w *writeOffset) Sequence() (int64, error) {
	return int64(w.seq), nil
}

func (w *writeOffset) AckIt() error {
	return fmt.Errorf("not supported")
}

func (w *writeOffset) NoAck() error {
	return fmt.Errorf("not supported")
}

func (w *writeOffset) PartitionIdx() int32 {
	return w.partitionIdx
}
