package partition

import (
	"github.com/numaproj/numaflow/pkg/window"
	"sync"
)

type Partition struct {
	// Interval is the window for this partition
	Interval window.IntervalWindow
	// Key is the key associated with messages in a keyed stream
	Key string
	// ID uniquely identifies a partition
	ID string
}

type Partitioner struct {
	activePartitions map[window.IntervalWindow][]*Partition
	lock             sync.Mutex
}

// NewPartitioner creates a new partitioner
func NewPartitioner() *Partitioner {
	partitioner := &Partitioner{
		activePartitions: make(map[window.IntervalWindow][]*Partition),
	}

	return partitioner
}

func (p *Partitioner) addPartition(key string, interval window.IntervalWindow) {
	partition := newPartition(key, interval)
	if _, ok := p.activePartitions[interval]; !ok {
		p.activePartitions[interval] = make([]*Partition, 0)
	}

	p.activePartitions[interval] = append(p.activePartitions[interval], partition)
}

func newPartition(key string, interval window.IntervalWindow) *Partition {
	partition := &Partition{
		Key:      key,
		Interval: interval,
	}

	return partition
}
