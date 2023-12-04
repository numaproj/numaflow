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

package utils

import (
	"strconv"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"

	"github.com/numaproj/numaflow/pkg/isb"
)

// DefaultPartitionIdx Default partition index
var DefaultPartitionIdx = int32(0)

// simpleSourceOffset is a simple implementation of isb.Offset from the source side.
type simpleSourceOffset struct {
	offset       string
	partitionIdx int32
}

func NewSimpleSourceOffset(o string, p int32) isb.Offset {
	return &simpleSourceOffset{
		offset:       o,
		partitionIdx: p,
	}
}

func (s *simpleSourceOffset) String() string {
	return s.offset
}

func (s *simpleSourceOffset) PartitionIdx() int32 {
	return s.partitionIdx
}

func (s *simpleSourceOffset) Sequence() (int64, error) {
	panic("Sequence is not supported by simpleSourceOffset")
}

func (s *simpleSourceOffset) AckIt() error {
	panic("AckIt is not supported by simpleSourceOffset")
}

func (s *simpleSourceOffset) NoAck() error {
	panic("NoAck is not supported by simpleSourceOffset")
}

func ConvertToSourceOffset(offset isb.Offset) *sourcepb.Offset {
	return &sourcepb.Offset{
		PartitionId: strconv.Itoa(int(offset.PartitionIdx())),
		Offset:      []byte(offset.String()),
	}
}

func ConvertToIsbOffset(offset *sourcepb.Offset) isb.Offset {
	if partitionIdx, err := strconv.Atoi(offset.GetPartitionId()); err != nil {
		// If the partition ID is not a number, use the default partition index
		// TODO - should we require UDSource users to return us a number instead of string as partition ID?
		return NewSimpleSourceOffset(string(offset.Offset), DefaultPartitionIdx)
	} else {
		return NewSimpleSourceOffset(string(offset.Offset), int32(partitionIdx))
	}
}
