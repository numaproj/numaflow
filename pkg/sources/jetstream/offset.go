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
	"time"

	jetstreamlib "github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
)

// offset represents a message offset in a jetstream.
// The implementation is same as `pkg/isb/stores/jetstream/reader.go` except for the type of `msg` field.
// Once the ISB implementation starts using the new Jetstream client APIs, we can merge both.
type offset struct {
	msg        jetstreamlib.Msg
	seq        uint64
	cancelFunc context.CancelFunc
}

var _ isb.Offset = (*offset)(nil)

func newOffset(msg jetstreamlib.Msg, seqNum uint64, tickDuration time.Duration, log *zap.SugaredLogger) isb.Offset {
	o := &offset{
		msg: msg,
		seq: seqNum,
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
func (o *offset) workInProgress(ctx context.Context, msg jetstreamlib.Msg, tickDuration time.Duration, log *zap.SugaredLogger) {
	ticker := time.NewTicker(tickDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Debugw("Setting message processing is in progress", zap.Uint64("stream_sequence_number", o.seq))
			if err := msg.InProgress(); err != nil && !errors.Is(err, jetstreamlib.ErrMsgAlreadyAckd) && !errors.Is(err, jetstreamlib.ErrMsgNotFound) {
				log.Errorw("Failed to set JetStream message as in progress", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (o *offset) String() string {
	return fmt.Sprintf("%d", o.seq)
}

func (o *offset) Sequence() (int64, error) {
	return int64(o.seq), nil
}

func (o *offset) AckIt() error {
	if o.cancelFunc != nil {
		o.cancelFunc()
	}
	if err := o.msg.Ack(); err != nil && !errors.Is(err, jetstreamlib.ErrMsgAlreadyAckd) && !errors.Is(err, jetstreamlib.ErrMsgNotFound) {
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

func (o *offset) PartitionIdx() int32 {
	return 0
}
