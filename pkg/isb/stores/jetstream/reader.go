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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type jetStreamReader struct {
	name                   string
	stream                 string
	subject                string
	client                 *jsclient.Client
	sub                    *nats.Subscription
	opts                   *readOptions
	inProgressTickDuration time.Duration
	partitionIdx           int32
	log                    *zap.SugaredLogger
}

// NewJetStreamBufferReader is used to provide a new JetStream buffer reader connection
func NewJetStreamBufferReader(ctx context.Context, client *jsclient.Client, name, stream, subject string, partitionIdx int32, opts ...ReadOption) (isb.BufferReader, error) {
	log := logging.FromContext(ctx).With("bufferReader", name).With("stream", stream).With("subject", subject)
	o := defaultReadOptions()
	for _, opt := range opts {
		if opt != nil {
			if err := opt(o); err != nil {
				return nil, err
			}
		}
	}
	reader := &jetStreamReader{
		name:         name,
		stream:       stream,
		subject:      subject,
		client:       client,
		partitionIdx: partitionIdx,
		opts:         o,
		log:          log,
	}

	jsContext, err := reader.client.JetStreamContext()
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context, %w", err)
	}

	consumer, err := jsContext.ConsumerInfo(stream, stream)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer info, %w", err)
	}

	// If ackWait is 3s, ticks every 2s.
	inProgressTickSeconds := int64(consumer.Config.AckWait.Seconds() * 2 / 3)
	if inProgressTickSeconds < 1 {
		inProgressTickSeconds = 1
	}

	sub, err := reader.client.Subscribe(subject, stream, nats.Bind(stream, stream))
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to subject %q, %w", subject, err)
	}

	reader.sub = sub
	reader.inProgressTickDuration = time.Duration(inProgressTickSeconds * int64(time.Second))
	return reader, nil
}

func (jr *jetStreamReader) GetName() string {
	return jr.name
}

func (jr *jetStreamReader) GetPartitionIdx() int32 {
	return jr.partitionIdx
}

func (jr *jetStreamReader) Close() error {
	if jr.sub != nil {
		if err := jr.sub.Unsubscribe(); err != nil {
			jr.log.Errorw("Failed to unsubscribe", zap.Error(err))
		}
	}
	return nil
}

func (jr *jetStreamReader) Pending(_ context.Context) (int64, error) {
	return jr.client.PendingForStream(jr.stream, jr.stream)
}

func (jr *jetStreamReader) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	labels := map[string]string{"buffer": jr.GetName()}
	defer func(t time.Time) {
		isbReadTime.With(labels).Observe(float64(time.Since(t).Microseconds()))
	}(time.Now())
	var err error
	var result []*isb.ReadMessage
	msgs, err := jr.sub.Fetch(int(count), nats.MaxWait(jr.opts.readTimeOut))
	if err != nil && !errors.Is(err, nats.ErrTimeout) {
		isbReadErrors.With(map[string]string{"buffer": jr.GetName()}).Inc()
		return nil, fmt.Errorf("failed to fetch messages from jet stream subject %q, %w", jr.subject, err)
	}
	for _, msg := range msgs {
		var m = new(isb.Message)
		// err should be nil as we have our own marshaller/unmarshaller
		err = m.UnmarshalBinary(msg.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal the message into isb.Message, %w", err)
		}
		msgMetadata, err := msg.Metadata()
		if err != nil {
			return nil, fmt.Errorf("failed to get jetstream message metadata, %w", err)
		}
		rm := &isb.ReadMessage{
			ReadOffset: newOffset(msg, jr.inProgressTickDuration, jr.partitionIdx, jr.log),
			Message:    *m,
			Metadata: isb.MessageMetadata{
				NumDelivered: msgMetadata.NumDelivered,
			},
		}
		result = append(result, rm)
	}
	return result, nil
}

func (jr *jetStreamReader) Ack(_ context.Context, offsets []isb.Offset) []error {
	labels := map[string]string{"buffer": jr.GetName()}
	defer func(t time.Time) {
		isbAckTime.With(labels).Observe(float64(time.Since(t).Microseconds()))
	}(time.Now())

	if len(offsets) == 1 {
		if err := offsets[0].AckIt(); err != nil {
			jr.log.Errorw("Failed to ack message", zap.Error(err))
			// If the error is related to nats/jetstream, we skip it because it might end up with infinite ack retries.
			// Skipping those errors to let the whole read/write/ack for loop to restart from reading, to pick up those
			// redelivered messages.
			if !strings.HasPrefix(err.Error(), "nats:") {
				return []error{err}
			}
		}
		return nil
	}
	errs := make([]error, len(offsets))
	done := make(chan struct{})
	wg := &sync.WaitGroup{}
	for idx, o := range offsets {
		wg.Add(1)
		go func(index int, o isb.Offset) {
			defer wg.Done()
			if err := o.AckIt(); err != nil {
				jr.log.Errorw("Failed to ack message", zap.Error(err))
				// If the error is related to nats/jetstream, we skip it because it might end up with infinite ack retries.
				// Skipping those errors to let the whole read/write/ack for loop to restart from reading, to pick up those
				// redelivered messages.
				if !strings.HasPrefix(err.Error(), "nats:") {
					errs[index] = err
				}
			}
		}(idx, o)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	<-done
	return errs
}

func (jr *jetStreamReader) NoAck(ctx context.Context, offsets []isb.Offset) {
	wg := &sync.WaitGroup{}
	for _, o := range offsets {
		wg.Add(1)
		go func(o isb.Offset) {
			defer wg.Done()
			// Ignore the returned error as the worst case the message will
			// take longer to be redelivered.
			if err := o.NoAck(); err != nil {
				jr.log.Errorw("Failed to nak JetStream msg", zap.Error(err))
			}
		}(o)
	}
	wg.Wait()
}

// offset implements ID interface for JetStream.
type offset struct {
	seq          uint64
	msg          *nats.Msg
	partitionIdx int32
	cancelFunc   context.CancelFunc
}

func newOffset(msg *nats.Msg, tickDuration time.Duration, partitionIdx int32, log *zap.SugaredLogger) *offset {
	metadata, _ := msg.Metadata()
	o := &offset{
		seq:          metadata.Sequence.Stream,
		msg:          msg,
		partitionIdx: partitionIdx,
	}
	// If tickDuration is 1s, which means ackWait is 1s or 2s, it does not make much sense to do it, instead, increasing ackWait is recommended.
	if tickDuration.Seconds() > 1 {
		ctx, cancel := context.WithCancel(context.Background())
		go o.workInProgress(ctx, msg, tickDuration, log)
		o.cancelFunc = cancel
	}
	return o
}

// workInProgress tells the jetstream server that this message is being worked on
// and resets the redelivery timer on the server, every tickDuration.
func (o *offset) workInProgress(ctx context.Context, msg *nats.Msg, tickDuration time.Duration, log *zap.SugaredLogger) {
	ticker := time.NewTicker(tickDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Debugw("Mark message processing in generic", zap.Any("seq", o.seq))
			if err := msg.InProgress(); err != nil && !errors.Is(err, nats.ErrMsgAlreadyAckd) && !errors.Is(err, nats.ErrMsgNotFound) {
				log.Errorw("Failed to set JetStream msg in generic", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (o *offset) String() string {
	return fmt.Sprint(o.seq) + "-" + fmt.Sprint(o.partitionIdx)
}

func (o *offset) AckIt() error {
	if o.cancelFunc != nil {
		o.cancelFunc()
	}
	if err := o.msg.AckSync(); err != nil && !errors.Is(err, nats.ErrMsgAlreadyAckd) && !errors.Is(err, nats.ErrMsgNotFound) {
		return err
	}
	return nil
}

func (o *offset) NoAck() error {
	if o.cancelFunc != nil {
		o.cancelFunc()
	}
	return o.msg.Nak()
}

func (o *offset) Sequence() (int64, error) {
	return int64(o.seq), nil
}

func (o *offset) PartitionIdx() int32 {
	return o.partitionIdx
}
