package wmb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
)

func TestNewSharedIdleManager(t *testing.T) {
	type args struct {
		fromBufferPartitions []string
		numOfToPartitions    int
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
				fromBufferPartitions: []string{"testFromBufferPartition0", "testFromBufferPartition1"},
				numOfToPartitions:    2,
			},
			want: &sharedIdleManager{
				wmbOffset: make(map[string]isb.Offset, 2),
				forwarderActiveToPartition: map[string]map[string]bool{
					"testFromBufferPartition0": make(map[string]bool),
					"testFromBufferPartition1": make(map[string]bool),
				},
				lock: sync.RWMutex{},
			},
			wantErr: "",
		},
		{
			name: "bad",
			args: args{
				fromBufferPartitions: nil,
				numOfToPartitions:    2,
			},
			want:    nil,
			wantErr: "missing fromBufferPartitions to create a new shared idle manager",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSharedIdleManager(tt.args.fromBufferPartitions, tt.args.numOfToPartitions)
			if err != nil {
				assert.Equal(t, tt.wantErr, err.Error())
			} else {
				// if err is nil, then wantErr must be empty
				assert.Equal(t, "", tt.wantErr)
			}
			assert.Equalf(t, tt.want, got, "NewSharedIdleManager(%v, %v)", tt.args.fromBufferPartitions, tt.args.numOfToPartitions)
		})
	}
}

func Test_sharedIdleManager_Get(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]map[string]bool
		lock                       sync.RWMutex
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
				forwarderActiveToPartition: map[string]map[string]bool{
					"testFromBufferPartition0": {
						"testToBufferPartition0": false,
					},
					"testFromBufferPartition1": {
						"testToBufferPartition0": false,
					},
				},
				lock: sync.RWMutex{},
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
				lock:                       tt.fields.lock,
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
		forwarderActiveToPartition map[string]map[string]bool
		lock                       sync.RWMutex
	}
	type args struct {
		toBufferPartitionName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &sharedIdleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       tt.fields.lock,
			}
			assert.Equalf(t, tt.want, im.NeedToSendCtrlMsg(tt.args.toBufferPartitionName), "NeedToSendCtrlMsg(%v)", tt.args.toBufferPartitionName)
		})
	}
}

func Test_sharedIdleManager_Reset(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]map[string]bool
		lock                       sync.RWMutex
	}
	type args struct {
		fromBufferPartitionName string
		toBufferPartitionName   string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &sharedIdleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       tt.fields.lock,
			}
			im.Reset(tt.args.fromBufferPartitionName, tt.args.toBufferPartitionName)
		})
	}
}

func Test_sharedIdleManager_Update(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]map[string]bool
		lock                       sync.RWMutex
	}
	type args struct {
		fromBufferPartitionName string
		toBufferPartitionName   string
		newOffset               isb.Offset
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &sharedIdleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       tt.fields.lock,
			}
			im.Update(tt.args.fromBufferPartitionName, tt.args.toBufferPartitionName, tt.args.newOffset)
		})
	}
}
