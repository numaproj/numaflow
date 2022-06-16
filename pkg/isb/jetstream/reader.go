package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

type jetStreamReader struct {
	name                  string
	stream                string
	subject               string
	conn                  *nats.Conn
	js                    nats.JetStreamContext
	sub                   *nats.Subscription
	opts                  *readOptions
	inProgessTickDuration time.Duration
	log                   *zap.SugaredLogger
	// ackedInfo stores a list of ack seq/timestamp(seconds) information
	ackedInfo *sharedqueue.OverflowQueue[timestampedSequence]
}

// NewJetStreamBufferReader is used to provide a new JetStream buffer reader connection
func NewJetStreamBufferReader(ctx context.Context, client clients.JetStreamClient, name, stream, subject string, opts ...ReadOption) (isb.BufferReader, error) {
	connectAndSubscribe := func() (*nats.Conn, nats.JetStreamContext, *nats.Subscription, error) {
		conn, err := client.Connect(ctx)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get nats connection, %w", err)
		}
		js, err := conn.JetStream()
		if err != nil {
			conn.Close()
			return nil, nil, nil, fmt.Errorf("failed to get jetstream context, %w", err)
		}
		sub, err := js.PullSubscribe(subject, stream, nats.Bind(stream, stream))
		if err != nil {
			conn.Close()
			return nil, nil, nil, fmt.Errorf("failed to subscribe jet stream subject %q, %w", subject, err)
		}
		return conn, js, sub, nil
	}
	conn, js, sub, err := connectAndSubscribe()
	if err != nil {
		return nil, err
	}

	o := defaultReadOptions()
	for _, opt := range opts {
		if opt != nil {
			if err := opt(o); err != nil {
				return nil, err
			}
		}
	}
	log := logging.FromContext(ctx).With("bufferReader", name).With("stream", stream).With("subject", subject)
	consumer, err := js.ConsumerInfo(stream, stream)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get consumer info, %w", err)
	}
	// If ackWait is 3s, ticks every 2s.
	inProgessTickSeconds := int64(consumer.Config.AckWait.Seconds() * 2 / 3)
	if inProgessTickSeconds < 1 {
		inProgessTickSeconds = 1
	}

	result := &jetStreamReader{
		name:                  name,
		stream:                stream,
		subject:               subject,
		conn:                  conn,
		js:                    js,
		sub:                   sub,
		opts:                  o,
		inProgessTickDuration: time.Duration(inProgessTickSeconds * int64(time.Second)),
		log:                   log,
	}
	if o.useAckInfoAsRate {
		result.ackedInfo = sharedqueue.New[timestampedSequence](1800)
		go result.runAckInfomationChecker(ctx)
	}
	return result, nil
}

func (jr *jetStreamReader) GetName() string {
	return jr.name
}

func (jr *jetStreamReader) Close() error {
	if jr.sub != nil {
		if err := jr.sub.Unsubscribe(); err != nil {
			jr.log.Errorw("Failed to unsubscribe", zap.Error(err))
		}
	}
	if jr.conn != nil && !jr.conn.IsClosed() {
		jr.conn.Close()
	}
	return nil
}

func (jr *jetStreamReader) runAckInfomationChecker(ctx context.Context) {
	js, err := jr.conn.JetStream()
	if err != nil {
		// Let it exit if it fails to start the information checker
		jr.log.Fatal("Failed to get Jet Stream context, %w", err)
	}
	checkAckInfo := func() {
		c, err := js.ConsumerInfo(jr.stream, jr.stream)
		if err != nil {
			jr.log.Errorw("failed to get consumer info in the reader", zap.Error(err))
			return
		}
		ts := timestampedSequence{seq: int64(c.AckFloor.Stream), timestamp: time.Now().Unix()}
		jr.ackedInfo.Append(ts)
		jr.log.Debugw("Acknowledge information", zap.Int64("ackSeq", ts.seq), zap.Int64("timestamp", ts.timestamp))
	}
	checkAckInfo()

	ticker := time.NewTicker(jr.opts.ackInfoCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			checkAckInfo()
		case <-ctx.Done():
			return
		}
	}
}

func (jr *jetStreamReader) Pending(_ context.Context) (int64, error) {
	c, err := jr.js.ConsumerInfo(jr.stream, jr.stream)
	if err != nil {
		return isb.PendingNotAvailable, fmt.Errorf("failed to get consumer info, %w", err)
	}
	return int64(c.NumPending), nil
}

// Rate returns the ack rate (tps)
func (jr *jetStreamReader) Rate(_ context.Context) (float64, error) {
	if !jr.opts.useAckInfoAsRate {
		return isb.RateNotAvailable, nil
	}
	timestampedSeqs := jr.ackedInfo.Items()
	if len(timestampedSeqs) < 2 {
		return isb.RateNotAvailable, nil
	}
	endSeqInfo := timestampedSeqs[len(timestampedSeqs)-1]
	startSeqInfo := timestampedSeqs[len(timestampedSeqs)-2]
	for i := len(timestampedSeqs) - 3; i >= 0; i-- {
		if endSeqInfo.timestamp-timestampedSeqs[i].timestamp > jr.opts.rateLookbackSeconds {
			startSeqInfo = timestampedSeqs[i]
			break
		}
	}
	// Check if it is too stale (use lookbackSeconds + 2 * interval to determine)
	if endSeqInfo.timestamp-startSeqInfo.timestamp > jr.opts.rateLookbackSeconds+2*int64(jr.opts.ackInfoCheckInterval.Seconds()) {
		return isb.RateNotAvailable, nil
	}
	return float64(endSeqInfo.seq-startSeqInfo.seq) / float64(endSeqInfo.timestamp-startSeqInfo.timestamp), nil
}

func (jr *jetStreamReader) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	result := []*isb.ReadMessage{}
	msgs, err := jr.sub.Fetch(int(count), nats.MaxWait(jr.opts.readTimeOut))
	if err != nil && !errors.Is(err, nats.ErrTimeout) {
		isbReadErrors.With(map[string]string{"buffer": jr.GetName()}).Inc()
		return nil, fmt.Errorf("failed to fetch messages from jet stream subject %q, %w", jr.subject, err)
	}
	for _, msg := range msgs {
		m := &isb.ReadMessage{
			ReadOffset: newOffset(msg, jr.inProgessTickDuration, jr.log),
			Message: isb.Message{
				Header: convert2IsbMsgHeader(msg.Header),
				Body: isb.Body{
					Payload: msg.Data,
				},
			},
		}
		result = append(result, m)
	}
	return result, nil
}

func (jr *jetStreamReader) Ack(_ context.Context, offsets []isb.Offset) []error {
	errs := make([]error, len(offsets))
	done := make(chan struct{})
	wg := &sync.WaitGroup{}
	for idx, o := range offsets {
		wg.Add(1)
		go func(index int, o isb.Offset) {
			defer wg.Done()
			if err := o.AckIt(); err != nil {
				jr.log.Errorw("Failed to ack message", zap.Error(err))
				errs[index] = err
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

func convert2IsbMsgHeader(header nats.Header) isb.Header {
	r := isb.Header{}
	if header.Get(_window) == "1" {
		r.IsWindow = true
	}
	if x := header.Get(_id); x != "" {
		r.ID = x
	}
	if x := header.Get(_key); x != "" {
		r.Key = []byte(x)
	}
	if x := header.Get(_eventTime); x != "" {
		i, _ := strconv.ParseInt(x, 10, 64)
		r.EventTime = time.UnixMilli(i)
	}
	if x := header.Get(_startTime); x != "" {
		i, _ := strconv.ParseInt(x, 10, 64)
		r.StartTime = time.UnixMilli(i)
	}
	if x := header.Get(_endTime); x != "" {
		i, _ := strconv.ParseInt(x, 10, 64)
		r.EndTime = time.UnixMilli(i)
	}
	return r
}

// offset implements ID interface for JetStream.
type offset struct {
	seq        uint64
	msg        *nats.Msg
	cancelFunc context.CancelFunc
}

func newOffset(msg *nats.Msg, tickDuration time.Duration, log *zap.SugaredLogger) *offset {
	metadata, _ := msg.Metadata()
	o := &offset{
		seq: metadata.Sequence.Stream,
		msg: msg,
	}
	// If tickDuration is 1s, which means ackWait is 1s or 2s, it doesn not make much sense to do it, instead, increasing ackWait is recommended.
	if tickDuration.Seconds() > 1 {
		ctx, cancel := context.WithCancel(context.Background())
		go o.workInProgess(ctx, msg, tickDuration, log)
		o.cancelFunc = cancel
	}
	return o
}

func (o *offset) workInProgess(ctx context.Context, msg *nats.Msg, tickDuration time.Duration, log *zap.SugaredLogger) {
	ticker := time.NewTicker(tickDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Debugw("Mark message processing in progress", zap.Any("seq", o.seq))
			if err := msg.InProgress(); err != nil && !errors.Is(err, nats.ErrMsgAlreadyAckd) && !errors.Is(err, nats.ErrMsgNotFound) {
				log.Errorw("Failed to set JetStream msg in progress", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (o *offset) String() string {
	return fmt.Sprint(o.seq)
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

func (o *offset) Sequence() (int64, error) {
	return int64(o.seq), nil
}
