package util

import (
	"fmt"
	"strconv"

	"github.com/numaproj/numaflow/pkg/isb"
)

// simpleIntPartitionOffset is a simple implementation of Offset interface which contains a sequence number in the form of integer and a partition index.
type simpleIntPartitionOffset struct {
	seq          int64
	partitionIdx int32
}

func NewSimpleIntPartitionOffset(seq int64, partitionIdx int32) isb.Offset {
	return &simpleIntPartitionOffset{
		seq:          seq,
		partitionIdx: partitionIdx,
	}
}

func (s *simpleIntPartitionOffset) String() string {
	return fmt.Sprintf("%d-%d", s.seq, s.partitionIdx)
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

func NewSimpleStringPartitionOffset(seq string, partitionIdx int32) isb.Offset {
	return &simpleStringPartitionOffset{
		seq:          seq,
		partitionIdx: partitionIdx,
	}
}

func (s *simpleStringPartitionOffset) String() string {
	return fmt.Sprintf("%s-%d", s.seq, s.partitionIdx)
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
