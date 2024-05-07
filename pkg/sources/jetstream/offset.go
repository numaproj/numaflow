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
	"fmt"

	jetstreamlib "github.com/nats-io/nats.go/jetstream"

	"github.com/numaproj/numaflow/pkg/isb"
)

type offset struct {
	msg         jetstreamlib.Msg
	metadata    *jetstreamlib.MsgMetadata
	partitionID int32
}

var _ isb.Offset = (*offset)(nil)

func newOffset(msg jetstreamlib.Msg, metadata *jetstreamlib.MsgMetadata, partitionID int32) isb.Offset {
	return &offset{
		msg:         msg,
		metadata:    metadata,
		partitionID: partitionID,
	}
}

func (o *offset) String() string {
	return fmt.Sprintf("%s:%d", o.metadata.Stream, o.metadata.Sequence.Stream)
}

func (o *offset) Sequence() (int64, error) {
	return int64(o.metadata.Sequence.Stream), nil
}

func (o *offset) AckIt() error {
	return o.msg.Ack()
}

func (o *offset) NoAck() error {
	return o.msg.Nak()
}

func (o *offset) PartitionIdx() int32 {
	return o.partitionID
}
