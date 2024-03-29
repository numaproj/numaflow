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

package udsource

import (
	"encoding/base64"
	"fmt"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"

	"github.com/numaproj/numaflow/pkg/isb"
)

// userDefinedSourceOffset is a implementation of isb.Offset from the user-defined source side.
type userDefinedSourceOffset struct {
	// NOTE: offset is base64 encoded string because we use offset to construct message ID
	// and message ID is a string.
	offset       string
	partitionIdx int32
}

func NewUserDefinedSourceOffset(offset *sourcepb.Offset) isb.Offset {
	return &userDefinedSourceOffset{
		offset:       base64.StdEncoding.EncodeToString(offset.GetOffset()),
		partitionIdx: offset.GetPartitionId(),
	}
}

func (s *userDefinedSourceOffset) String() string {
	return fmt.Sprintf("%s-%d", s.offset, s.partitionIdx)
}

func (s *userDefinedSourceOffset) PartitionIdx() int32 {
	return s.partitionIdx
}

func (s *userDefinedSourceOffset) Sequence() (int64, error) {
	panic("Sequence is not supported by userDefinedSourceOffset")
}

func (s *userDefinedSourceOffset) AckIt() error {
	panic("AckIt is not supported by userDefinedSourceOffset")
}

func (s *userDefinedSourceOffset) NoAck() error {
	panic("NoAck is not supported by userDefinedSourceOffset")
}

func ConvertToUserDefinedSourceOffset(offset isb.Offset) *sourcepb.Offset {
	decoded, _ := base64.StdEncoding.DecodeString(offset.(*userDefinedSourceOffset).offset)
	return &sourcepb.Offset{
		PartitionId: offset.PartitionIdx(),
		Offset:      decoded,
	}
}
