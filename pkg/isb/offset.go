package isb

import (
	"fmt"
	"strconv"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

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

func NewSimpleStringPartitionOffset(seq string, partitionIdx int32) Offset {
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
	return v1alpha1.DefaultPartitionIdx
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
	return v1alpha1.DefaultPartitionIdx
}
