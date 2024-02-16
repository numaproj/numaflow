package wmb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
)

func TestNewSharedIdleManager(t *testing.T) {
	type args struct {
		numOfFromPartitions int
		numOfToPartitions   int
	}
	tests := []struct {
		name    string
		args    args
		want    IdleManager
		wantErr string
	}{
		{
			name: "good",
			args: args{
				numOfFromPartitions: 2,
				numOfToPartitions:   2,
			},
			want: &sharedIdleManager{
				wmbOffset:                  make(map[string]isb.Offset, 2),
				forwarderActiveToPartition: make(map[string]int64, 2),
				lock:                       sync.RWMutex{},
			},
			wantErr: "",
		},
		{
			name: "bad",
			args: args{
				numOfFromPartitions: 1,
				numOfToPartitions:   2,
			},
			want:    nil,
			wantErr: "failed to create a new shared idle manager: numOfFromPartitions should be > 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSharedIdleManager(tt.args.numOfFromPartitions, tt.args.numOfToPartitions)
			if err != nil {
				assert.Equal(t, tt.wantErr, err.Error())
			} else {
				// if err is nil, then wantErr must be empty
				assert.Equal(t, "", tt.wantErr)
			}
			assert.Equalf(t, tt.want, got, "NewSharedIdleManager(%v, %v)", tt.args.numOfFromPartitions, tt.args.numOfToPartitions)
		})
	}
}

func Test_sharedIdleManager_Get(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]int64
	}
	type args struct {
		toBufferPartitionName string
	}
	var (
		o = isb.SimpleIntOffset(func() int64 {
			return int64(100)
		})
		sequence, _ = o.Sequence()
	)
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{
			name: "good",
			fields: fields{
				wmbOffset: map[string]isb.Offset{
					"testToBufferPartition0": o,
				},
				forwarderActiveToPartition: map[string]int64{
					// all forwarders are inactive
					"testToBufferPartition0": 0,
				},
			},
			args: args{
				toBufferPartitionName: "testToBufferPartition0",
			},
			want: sequence,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &sharedIdleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       sync.RWMutex{},
			}
			gotOffset := im.Get(tt.args.toBufferPartitionName)
			gotSequence, err := gotOffset.Sequence()
			assert.Nil(t, err)
			assert.Equalf(t, tt.want, gotSequence, "Get(%v)", tt.args.toBufferPartitionName)
		})
	}
}

func Test_sharedIdleManager_NeedToSendCtrlMsg(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]int64
	}
	type args struct {
		toBufferPartitionName string
	}
	var o = isb.SimpleIntOffset(func() int64 {
		return int64(100)
	})
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "send_new_idle",
			fields: fields{
				wmbOffset: map[string]isb.Offset{},
				forwarderActiveToPartition: map[string]int64{
					// all forwarders are inactive
					"testToBufferPartition0": 0,
				},
			},
			args: args{
				toBufferPartitionName: "testToBufferPartition0",
			},
			want: true,
		},
		{
			name: "dont_send_already_idle",
			fields: fields{
				wmbOffset: map[string]isb.Offset{
					"testToBufferPartition0": o,
				},
				forwarderActiveToPartition: map[string]int64{
					// all forwarders are inactive
					"testToBufferPartition0": 0,
				},
			},
			args: args{
				toBufferPartitionName: "testToBufferPartition0",
			},
			want: false,
		},
		{
			name: "dont_send_at_least_one_active",
			fields: fields{
				wmbOffset: map[string]isb.Offset{},
				forwarderActiveToPartition: map[string]int64{
					// forwarder0 is inactive while forwarder1 is active -> binary 10 -> decimal 2
					"testToBufferPartition0": 2,
				},
			},
			args: args{
				toBufferPartitionName: "testToBufferPartition0",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &sharedIdleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       sync.RWMutex{},
			}
			assert.Equalf(t, tt.want, im.NeedToSendCtrlMsg(tt.args.toBufferPartitionName), "NeedToSendCtrlMsg(%v)", tt.args.toBufferPartitionName)
		})
	}
}

func Test_sharedIdleManager_Reset(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]int64
	}
	type args struct {
		fromBufferPartitionIndex int32
		toBufferPartitionName    string
	}
	var (
		o = isb.SimpleIntOffset(func() int64 {
			return int64(100)
		})
		sequence, _ = o.Sequence()
		testMap     = map[string]int64{
			// forwarder0 is inactive while forwarder1 is active -> binary 10 -> decimal 2
			"testToBufferPartition0": 2,
		}
	)
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "good",
			fields: fields{
				wmbOffset: map[string]isb.Offset{
					"testToBufferPartition0": o, // should be reset to nil
				},
				forwarderActiveToPartition: testMap,
			},
			args: args{
				fromBufferPartitionIndex: 0,
				toBufferPartitionName:    "testToBufferPartition0",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &sharedIdleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       sync.RWMutex{},
			}
			// before
			beforeReset, err := im.Get(tt.args.toBufferPartitionName).Sequence()
			assert.Nil(t, err)
			assert.Equal(t, sequence, beforeReset)
			assert.Equal(t, int64(2), testMap[tt.args.toBufferPartitionName])
			// reset
			im.Reset(tt.args.fromBufferPartitionIndex, tt.args.toBufferPartitionName)
			// after
			assert.Nil(t, im.Get(tt.args.toBufferPartitionName))
			// now both bits should be 1 -> 11 -> 3
			assert.Equal(t, int64(3), testMap[tt.args.toBufferPartitionName])
		})
	}
}

func Test_sharedIdleManager_Update(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]int64
	}
	type args struct {
		fromBufferPartitionIndex int32
		toBufferPartitionName    string
		newOffset                isb.Offset
	}
	var (
		o = isb.SimpleIntOffset(func() int64 {
			return int64(100)
		})
		sequence, _ = o.Sequence()
		testMap     = map[string]int64{
			// forwarder0 is active while forwarder1 is inactive -> binary 01 -> decimal 1
			"testToBufferPartition0": 1,
		}
	)
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "good",
			fields: fields{
				wmbOffset:                  map[string]isb.Offset{},
				forwarderActiveToPartition: testMap,
			},
			args: args{
				fromBufferPartitionIndex: 0,
				toBufferPartitionName:    "testToBufferPartition0",
				newOffset:                o,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &sharedIdleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       sync.RWMutex{},
			}
			// before
			assert.Nil(t, im.Get(tt.args.toBufferPartitionName))
			assert.Equal(t, int64(1), testMap[tt.args.toBufferPartitionName])
			// update
			im.Update(tt.args.fromBufferPartitionIndex, tt.args.toBufferPartitionName, tt.args.newOffset)
			// after
			afterUpdate, err := im.Get(tt.args.toBufferPartitionName).Sequence()
			assert.Nil(t, err)
			assert.Equal(t, sequence, afterUpdate)
			// now the bits should be 0 -> 00 -> 0
			assert.Equal(t, int64(0), testMap[tt.args.toBufferPartitionName])
		})
	}
}
