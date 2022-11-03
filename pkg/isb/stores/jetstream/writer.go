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

	"github.com/numaproj/numaflow/pkg/isb"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

type jetStreamWriter struct {
	name    string
	stream  string
	subject string
	conn    *jsclient.NatsConn
	js      *jsclient.JetStreamContext
	opts    *writeOptions
	log     *zap.SugaredLogger

	isFull *atomic.Bool
	// writtenInfo stores a list of written seq/timestamp(seconds) information
	writtenInfo *sharedqueue.OverflowQueue[timestampedSequence]
}

// NewJetStreamBufferWriter is used to provide a new instance of JetStreamBufferWriter
func NewJetStreamBufferWriter(ctx context.Context, client jsclient.JetStreamClient, name, stream, subject string, opts ...WriteOption) (isb.BufferWriter, error) {
	o := defaultWriteOptions()
	for _, opt := range opts {
		if opt != nil {
			if err := opt(o); err != nil {
				return nil, err
			}
		}
	}
	conn, err := client.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get nats connection, %w", err)
	}

	js, err := conn.JetStream(nats.PublishAsyncMaxPending(1024))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get JetStream context for writer")
	}

	result := &jetStreamWriter{
		name:    name,
		stream:  stream,
		subject: subject,
		conn:    conn,
		js:      js,
		opts:    o,
		isFull:  atomic.NewBool(true),
		log:     logging.FromContext(ctx).With("bufferWriter", name).With("stream", stream).With("subject", subject),
	}

	if o.useWriteInfoAsRate {
		result.writtenInfo = sharedqueue.New[timestampedSequence](3600)
	}

	go result.runStatusChecker(ctx)
	return result, nil
}

func (jw *jetStreamWriter) runStatusChecker(ctx context.Context) {
	labels := map[string]string{"buffer": jw.GetName()}
	// Use a separated JetStrea context for status checker
	js, err := jw.conn.JetStream()
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
		if jw.opts.useWriteInfoAsRate {
			ts := timestampedSequence{seq: int64(s.State.LastSeq), timestamp: time.Now().Unix()}
			jw.writtenInfo.Append(ts)
			jw.log.Debugw("Written information", zap.Int64("lastSeq", ts.seq), zap.Int64("timestamp", ts.timestamp))
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

func (jw *jetStreamWriter) Close() error {
	if jw.conn != nil && !jw.conn.IsClosed() {
		jw.conn.Close()
	}
	return nil
}

// Rate returns the writting rate (tps) in the past seconds
func (jw *jetStreamWriter) Rate(_ context.Context, seconds int64) (float64, error) {
	if !jw.opts.useWriteInfoAsRate {
		return isb.RateNotAvailable, nil
	}
	timestampedSeqs := jw.writtenInfo.Items()
	if len(timestampedSeqs) < 2 {
		return isb.RateNotAvailable, nil
	}
	endSeqInfo := timestampedSeqs[len(timestampedSeqs)-1]
	startSeqInfo := timestampedSeqs[len(timestampedSeqs)-2]
	now := time.Now().Unix()
	if now-startSeqInfo.timestamp > seconds {
		return isb.RateNotAvailable, nil
	}
	for i := len(timestampedSeqs) - 3; i >= 0; i-- {
		if now-timestampedSeqs[i].timestamp <= seconds {
			startSeqInfo = timestampedSeqs[i]
		} else {
			break
		}
	}
	return float64(endSeqInfo.seq-startSeqInfo.seq) / float64(endSeqInfo.timestamp-startSeqInfo.timestamp), nil
}

func (jw *jetStreamWriter) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	labels := map[string]string{"buffer": jw.GetName()}
	var errs = make([]error, len(messages))
	for i := 0; i < len(errs); i++ {
		errs[i] = fmt.Errorf("unknown error")
	}
	var writeOffsets = make([]isb.Offset, len(messages))
	if jw.isFull.Load() {
		jw.log.Debugw("Is full")
		isbFull.With(map[string]string{"buffer": jw.GetName()}).Inc()
		for i := 0; i < len(errs); i++ {
			errs[i] = isb.BufferWriteErr{Name: jw.name, Full: true, Message: "Buffer full!"}
		}
		isbWriteErrors.With(labels).Inc()
		return nil, errs
	}

	var futures = make([]nats.PubAckFuture, len(messages))
	for index, message := range messages {
		m := &nats.Msg{
			Header:  convert2NatsMsgHeader(message.Header),
			Subject: jw.subject,
			Data:    message.Payload,
		}
		if future, err := jw.js.PublishMsgAsync(m, nats.MsgId(message.Header.ID)); err != nil { // nats.MsgId() is for exactly-once writing
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
				writeOffsets[idx] = &writeOffset{seq: pubAck.Sequence}
				errs[idx] = nil
				jw.log.Debugw("Succeeded to publish a message", zap.String("stream", pubAck.Stream), zap.Any("seq", pubAck.Sequence), zap.Bool("duplicate", pubAck.Duplicate), zap.String("domain", pubAck.Domain))
			case err := <-fu.Err():
				errs[idx] = err
				isbWriteErrors.With(labels).Inc()
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
		isbWriteTimeout.With(labels).Inc()
	}
	return writeOffsets, errs
}

// writeOffset is the offset of the location in the JS stream we wrote to.
type writeOffset struct {
	seq uint64
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

const (
	_key       = "k"
	_id        = "i"
	_late      = "l"
	_eventTime = "pev"
	_startTime = "ps"
	_endTime   = "pen"
)

func convert2NatsMsgHeader(header isb.Header) nats.Header {
	r := nats.Header{}
	r.Add(_id, header.ID)
	r.Add(_key, string(header.Key))
	if header.IsLate {
		r.Add(_late, "1")
	} else {
		r.Add(_late, "0")
	}
	if !header.EventTime.IsZero() {
		r.Add(_eventTime, fmt.Sprint(header.EventTime.UnixMilli()))
	}
	if !header.StartTime.IsZero() {
		r.Add(_startTime, fmt.Sprint(header.StartTime.UnixMilli()))
	}
	if !header.EndTime.IsZero() {
		r.Add(_endTime, fmt.Sprint(header.EndTime.UnixMilli()))
	}
	return r
}
