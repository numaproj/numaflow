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

package isb

import (
	"strconv"
)

// DefaultPartitionIdx Default partition index
var DefaultPartitionIdx = int32(0)

// simpleIntPartitionOffset is a simple implementation of Offset interface which contains a sequence number in the form of integer and a partition index.
type simpleIntPartitionOffset struct {
	seq          int64
	partitionIdx int32
}

func NewSimpleIntPartitionOffset(seq int64, partitionIdx int32) Offset {
	return &simpleIntPartitionOffset{
		seq:          seq,
		partitionIdx: partitionIdx,
	}
}

func (s *simpleIntPartitionOffset) String() string {
	return strconv.FormatInt(s.seq, 10) + "-" + strconv.Itoa(int(s.partitionIdx))
}

func (s *simpleIntPartitionOffset) Sequence() (int64, error) {
	return s.seq, nil
}

func (s *simpleIntPartitionOffset) AckIt() error {
	return nil
}

func (s *simpleIntPartitionOffset) NoAck() error {
	return nil
}

func (s *simpleIntPartitionOffset) PartitionIdx() int32 {
	return s.partitionIdx
}

// simpleStringPartitionOffset is a simple implementation of Offset interface which contains a sequence number in the form of string and a partition index.
type simpleStringPartitionOffset struct {
	seq          string
	partitionIdx int32
}

func NewSimpleStringPartitionOffset(seq string, partitionIdx int32) Offset {
	return &simpleStringPartitionOffset{
		seq:          seq,
		partitionIdx: partitionIdx,
	}
}

func (s *simpleStringPartitionOffset) String() string {
	return s.seq + "-" + strconv.Itoa(int(s.partitionIdx))
}

func (s *simpleStringPartitionOffset) Sequence() (int64, error) {
	return strconv.ParseInt(s.seq, 10, 64)
}

func (s *simpleStringPartitionOffset) AckIt() error {
	return nil
}

func (s *simpleStringPartitionOffset) NoAck() error {
	return nil
}

func (s *simpleStringPartitionOffset) PartitionIdx() int32 {
	return s.partitionIdx
}

// SimpleStringOffset is an Offset convenient function for implementations without needing AckIt() when offset is a string.
type SimpleStringOffset func() string

func (so SimpleStringOffset) String() string {
	return so()
}

func (so SimpleStringOffset) Sequence() (int64, error) {
	return strconv.ParseInt(so(), 10, 64)
}

func (so SimpleStringOffset) AckIt() error {
	return nil
}

func (so SimpleStringOffset) NoAck() error {
	return nil
}

func (so SimpleStringOffset) PartitionIdx() int32 {
	return DefaultPartitionIdx
}

// SimpleIntOffset is an Offset convenient function for implementations without needing AckIt() when offset is a int64.
type SimpleIntOffset func() int64

func (si SimpleIntOffset) String() string {
	return strconv.FormatInt(si(), 10)
}

func (si SimpleIntOffset) Sequence() (int64, error) {
	return si(), nil
}

func (si SimpleIntOffset) AckIt() error {
	return nil
}

func (si SimpleIntOffset) NoAck() error {
	return nil
}

func (si SimpleIntOffset) PartitionIdx() int32 {
	return DefaultPartitionIdx
}
