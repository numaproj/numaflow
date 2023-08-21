package utils

import (
	"github.com/numaproj/numaflow/pkg/isb"
)

// DefaultPartitionIdx Default partition index
var DefaultPartitionIdx = int32(0)

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
