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

package wmb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
)

func TestNewIdleManager(t *testing.T) {
	var (
		toBufferName = "testBuffer"
		o            = isb.SimpleIntOffset(func() int64 {
			return int64(100)
		})
	)
	im, _ := NewIdleManager(1, 10)
	assert.NotNil(t, im)
	assert.True(t, im.NeedToSendCtrlMsg(toBufferName))
	im.Update(0, toBufferName, o)
	getOffset := im.Get(toBufferName)
	assert.Equal(t, o.String(), getOffset.String())
	assert.False(t, im.NeedToSendCtrlMsg(toBufferName))
	im.MarkActive(0, toBufferName)
	assert.True(t, im.NeedToSendCtrlMsg(toBufferName))

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
			want: &idleManager{
				wmbOffset:                  make(map[string]isb.Offset, 2),
				forwarderActiveToPartition: make(map[string]uint64, 2),
				lock:                       sync.RWMutex{},
			},
			wantErr: "",
		},
		{
			name: "bad0",
			args: args{
				numOfFromPartitions: 0,
				numOfToPartitions:   2,
			},
			want:    nil,
			wantErr: "failed to create a new idle manager: numOfFromPartitions should be > 0 and < 64",
		},
		{
			name: "bad64",
			args: args{
				numOfFromPartitions: 64,
				numOfToPartitions:   2,
			},
			want:    nil,
			wantErr: "failed to create a new idle manager: numOfFromPartitions should be > 0 and < 64",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIdleManager(tt.args.numOfFromPartitions, tt.args.numOfToPartitions)
			if err != nil {
				assert.Equal(t, tt.wantErr, err.Error())
			} else {
				// if err is nil, then wantErr must be empty
				assert.Equal(t, "", tt.wantErr)
			}
			assert.Equalf(t, tt.want, got, "NewIdleManager(%v, %v)", tt.args.numOfFromPartitions, tt.args.numOfToPartitions)
		})
	}
}

func Test_idleManager_Get(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]uint64
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
				forwarderActiveToPartition: map[string]uint64{
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
			im := &idleManager{
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

func Test_idleManager_NeedToSendCtrlMsg(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]uint64
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
				forwarderActiveToPartition: map[string]uint64{
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
				forwarderActiveToPartition: map[string]uint64{
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
				forwarderActiveToPartition: map[string]uint64{
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
			im := &idleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       sync.RWMutex{},
			}
			assert.Equalf(t, tt.want, im.NeedToSendCtrlMsg(tt.args.toBufferPartitionName), "NeedToSendCtrlMsg(%v)", tt.args.toBufferPartitionName)
		})
	}
}

func Test_idleManager_Reset(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]uint64
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
		testMap     = map[string]uint64{
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
			im := &idleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       sync.RWMutex{},
			}
			// before
			beforeReset, err := im.Get(tt.args.toBufferPartitionName).Sequence()
			assert.Nil(t, err)
			assert.Equal(t, sequence, beforeReset)
			assert.Equal(t, uint64(2), testMap[tt.args.toBufferPartitionName])
			// reset
			im.MarkActive(tt.args.fromBufferPartitionIndex, tt.args.toBufferPartitionName)
			// after
			assert.Nil(t, im.Get(tt.args.toBufferPartitionName))
			// now both bits should be 1 -> 11 -> 3
			assert.Equal(t, uint64(3), testMap[tt.args.toBufferPartitionName])
		})
	}
}

func Test_idleManager_Update(t *testing.T) {
	type fields struct {
		wmbOffset                  map[string]isb.Offset
		forwarderActiveToPartition map[string]uint64
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
		testMap     = map[string]uint64{
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
			im := &idleManager{
				wmbOffset:                  tt.fields.wmbOffset,
				forwarderActiveToPartition: tt.fields.forwarderActiveToPartition,
				lock:                       sync.RWMutex{},
			}
			// before
			assert.Nil(t, im.Get(tt.args.toBufferPartitionName))
			assert.Equal(t, uint64(1), testMap[tt.args.toBufferPartitionName])
			// update
			im.Update(tt.args.fromBufferPartitionIndex, tt.args.toBufferPartitionName, tt.args.newOffset)
			// after
			afterUpdate, err := im.Get(tt.args.toBufferPartitionName).Sequence()
			assert.Nil(t, err)
			assert.Equal(t, sequence, afterUpdate)
			// now the bits should be 0 -> 00 -> 0
			assert.Equal(t, uint64(0), testMap[tt.args.toBufferPartitionName])
		})
	}
}
