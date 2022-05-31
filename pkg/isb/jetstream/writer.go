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
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type jetStreamWriter struct {
	name    string
	stream  string
	subject string
	conn    *nats.Conn
	js      nats.JetStreamContext
	opts    *writeOptions
	log     *zap.SugaredLogger

	isFull *atomic.Bool
}

// NewJetStreamBufferWriter is used to provide a new instance of JetStreamBufferWriter
func NewJetStreamBufferWriter(ctx context.Context, client clients.JetStreamClient, name, stream, subject string, opts ...WriteOption) (isb.BufferWriter, error) {
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
			isbIsFullErrors.With(labels).Inc()
			jw.log.Errorw("Failed to get stream info", zap.Error(err))
			return
		}
		c, err := js.ConsumerInfo(jw.stream, jw.stream)
		if err != nil {
			isbIsFullErrors.With(labels).Inc()
			jw.log.Errorw("failed to get consumer info", zap.Error(err))
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
		isbBufferSoftUsage.With(labels).Set(softUsage)
		isbBufferSolidUsage.With(labels).Set(solidUsage)
		isbBufferPending.With(labels).Set(float64(c.NumPending))
		isbBufferAckPending.With(labels).Set(float64(c.NumAckPending))
		isbBufferWaiting.With(labels).Set(float64(c.NumWaiting))
		isbBufferAckFloor.With(labels).Set(float64(c.AckFloor.Stream))

		jw.log.Infow("Consumption information", zap.Any("totalMsgs", s.State.Msgs), zap.Any("pending", c.NumPending),
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

func (jw *jetStreamWriter) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	labels := map[string]string{"buffer": jw.GetName()}
	var errs = make([]error, len(messages))
	var writeOffsets = make([]isb.Offset, len(messages))
	if jw.isFull.Load() {
		jw.log.Debugw("Is full")
		isbIsFull.With(map[string]string{"buffer": jw.GetName()}).Inc()
		for i := range errs {
			errs[i] = isb.BufferWriteErr{Name: jw.name, Full: true, Message: "Buffer full!"}
		}
		isbWriteErrors.With(labels).Inc()
		return nil, errs
	}

	wg := new(sync.WaitGroup)
	for index, msg := range messages {
		wg.Add(1)
		go func(message isb.Message, idx int) {
			defer wg.Done()
			m := &nats.Msg{
				Header:  convert2NatsMsgHeader(message.Header),
				Subject: jw.subject,
				Data:    message.Payload,
			}
			if pubAck, err := jw.js.PublishMsg(m, nats.MsgId(message.Header.ID)); err != nil { // nats.MsgId() is for exactly-once writing
				errs[idx] = err
				isbWriteErrors.With(labels).Inc()
			} else {
				writeOffsets[idx] = &writeOffset{seq: pubAck.Sequence}
				jw.log.Debugw("Succeeded to publish a message", zap.String("stream", pubAck.Stream), zap.Any("seq", pubAck.Sequence), zap.Bool("duplicate", pubAck.Duplicate), zap.String("domain", pubAck.Domain))
			}
		}(msg, index)
	}
	wg.Wait()
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
	_window    = "w"
	_eventTime = "pev"
	_startTime = "ps"
	_endTime   = "pen"
)

func convert2NatsMsgHeader(header isb.Header) nats.Header {
	r := nats.Header{}
	r.Add(_id, header.ID)
	r.Add(_key, string(header.Key))
	if header.IsWindow {
		r.Add(_window, "1")
	} else {
		r.Add(_window, "0")
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
