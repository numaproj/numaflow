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

package fetch

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"

	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

func TestBuffer_GetWatermark(t *testing.T) {
	var ctx = context.Background()

	// We don't really need watcher because we manually call the `Put` function and the `addProcessor` function
	// so use no op watcher for testing
	hbWatcher := noop.NewKVOpWatch()
	otWatcher := noop.NewKVOpWatch()
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	processorManager := processor.NewProcessorManager(ctx, storeWatcher)
	var (
		testPod0     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5)
		testPod1     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5)
		testPod2     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5)
		pod0Timeline = []wmb.WMB{
			{Watermark: 11, Offset: 9},
			{Watermark: 12, Offset: 20},
			{Watermark: 13, Offset: 21},
			{Watermark: 14, Offset: 22},
			{Watermark: 17, Offset: 28},
		}
		pod1Timeline = []wmb.WMB{
			{Watermark: 8, Offset: 13},
			{Watermark: 9, Offset: 16},
			{Watermark: 10, Offset: 18},
			{Watermark: 17, Offset: 26},
		}
		pod2Timeline = []wmb.WMB{
			{Watermark: 10, Offset: 14},
			{Watermark: 12, Offset: 17},
			{Watermark: 14, Offset: 19},
			{Watermark: 17, Offset: 24},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimeline().Put(watermark)
	}
	processorManager.AddProcessor("testPod0", testPod0)
	processorManager.AddProcessor("testPod1", testPod1)
	processorManager.AddProcessor("testPod2", testPod2)

	type args struct {
		offset int64
	}
	tests := []struct {
		name             string
		processorManager *processor.ProcessorManager
		args             args
		want             int64
	}{
		{
			name:             "offset_9",
			processorManager: processorManager,
			args:             args{9},
			want:             -1,
		},
		{
			name:             "offset_15",
			processorManager: processorManager,
			args:             args{15},
			want:             8,
		},
		{
			name:             "offset_18",
			processorManager: processorManager,
			args:             args{18},
			want:             9,
		},
		{
			name:             "offset_22",
			processorManager: processorManager,
			args:             args{22},
			want:             10,
		},
		{
			name:             "offset_23",
			processorManager: processorManager,
			args:             args{23},
			want:             10,
		},
		{
			name:             "offset_28",
			processorManager: processorManager,
			args:             args{28},
			want:             14,
		},
		{
			name:             "offset_29",
			processorManager: processorManager,
			args:             args{29},
			want:             17,
		},
	}
	location, _ := time.LoadLocation("UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &edgeFetcher{
				ctx:              ctx,
				bufferName:       "testBuffer",
				processorManager: tt.processorManager,
				log:              zaptest.NewLogger(t).Sugar(),
			}
			if got := b.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.args.offset, 10) })); time.Time(got).In(location) != time.UnixMilli(tt.want).In(location) {
				t.Errorf("GetWatermark() = %v, want %v", got, wmb.Watermark(time.UnixMilli(tt.want)))
			}
			// this will always be 17 because the timeline has been populated ahead of time
			// GetHeadWatermark is only used in UI and test
			assert.Equal(t, time.Time(b.GetHeadWatermark()).In(location), time.UnixMilli(17).In(location))
		})
	}
}

func Test_edgeFetcher_GetHeadWatermark(t *testing.T) {
	var (
		ctx               = context.Background()
		bufferName        = "testBuffer"
		hbWatcher         = noop.NewKVOpWatch()
		otWatcher         = noop.NewKVOpWatch()
		storeWatcher      = store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
		processorManager1 = processor.NewProcessorManager(ctx, storeWatcher)
		processorManager2 = processor.NewProcessorManager(ctx, storeWatcher)
	)

	getHeadWMTest1(ctx, processorManager1)
	getHeadWMTest2(ctx, processorManager2)

	tests := []struct {
		name             string
		processorManager *processor.ProcessorManager
		want             int64
	}{
		{
			name:             "all pods idle and get an idle WMB",
			processorManager: processorManager1,
			want:             14,
		},
		{
			name:             "some pods idle and skip an idle WMB",
			processorManager: processorManager2,
			want:             17,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &edgeFetcher{
				ctx:              ctx,
				bufferName:       bufferName,
				storeWatcher:     storeWatcher,
				processorManager: tt.processorManager,
				log:              zaptest.NewLogger(t).Sugar(),
			}
			assert.Equalf(t, tt.want, e.GetHeadWatermark().UnixMilli(), "GetHeadWatermark()")
		})
	}
}

func getHeadWMTest1(ctx context.Context, processorManager1 *processor.ProcessorManager) {
	var (
		testPod0     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5)
		testPod1     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5)
		testPod2     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    28,
				Watermark: 14,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 15,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    24,
				Watermark: 16,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimeline().Put(watermark)
	}
	processorManager1.AddProcessor("testPod0", testPod0)
	processorManager1.AddProcessor("testPod1", testPod1)
	processorManager1.AddProcessor("testPod2", testPod2)
}

func getHeadWMTest2(ctx context.Context, processorManager1 *processor.ProcessorManager) {
	var (
		testPod0     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5)
		testPod1     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5)
		testPod2     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 17,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    24,
				Watermark: 17,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimeline().Put(watermark)
	}
	processorManager1.AddProcessor("testPod0", testPod0)
	processorManager1.AddProcessor("testPod1", testPod1)
	processorManager1.AddProcessor("testPod2", testPod2)
}

func Test_edgeFetcher_GetHeadWMB(t *testing.T) {
	var (
		ctx               = context.Background()
		bufferName        = "testBuffer"
		hbWatcher         = noop.NewKVOpWatch()
		otWatcher         = noop.NewKVOpWatch()
		storeWatcher      = store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
		processorManager1 = processor.NewProcessorManager(ctx, storeWatcher)
		processorManager2 = processor.NewProcessorManager(ctx, storeWatcher)
		processorManager3 = processor.NewProcessorManager(ctx, storeWatcher)
		processorManager4 = processor.NewProcessorManager(ctx, storeWatcher)
	)

	getHeadWMBTest1(ctx, processorManager1)
	getHeadWMBTest2(ctx, processorManager2)
	getHeadWMBTest3(ctx, processorManager3)
	getHeadWMBTest4(ctx, processorManager4)

	tests := []struct {
		name             string
		processorManager *processor.ProcessorManager
		want             wmb.WMB
	}{
		{
			name:             "all pods idle and get an idle WMB",
			processorManager: processorManager1,
			want: wmb.WMB{
				Idle:      true,
				Offset:    24,
				Watermark: 17,
			},
		},
		{
			name:             "some pods idle and skip an idle WMB",
			processorManager: processorManager2,
			want:             wmb.WMB{},
		},
		{
			name:             "all pods not idle and skip an idle WMB",
			processorManager: processorManager3,
			want:             wmb.WMB{},
		},
		{
			name:             "all pods empty timeline",
			processorManager: processorManager3,
			want:             wmb.WMB{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &edgeFetcher{
				ctx:              ctx,
				bufferName:       bufferName,
				storeWatcher:     storeWatcher,
				processorManager: tt.processorManager,
				log:              zaptest.NewLogger(t).Sugar(),
			}
			assert.Equalf(t, tt.want, e.GetHeadWMB(), "GetHeadWMB()")
		})
	}
}

func getHeadWMBTest1(ctx context.Context, processorManager1 *processor.ProcessorManager) {
	var (
		testPod0     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5)
		testPod1     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5)
		testPod2     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    28,
				Watermark: 17,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 17,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    24,
				Watermark: 17,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimeline().Put(watermark)
	}
	processorManager1.AddProcessor("testPod0", testPod0)
	processorManager1.AddProcessor("testPod1", testPod1)
	processorManager1.AddProcessor("testPod2", testPod2)
}

func getHeadWMBTest2(ctx context.Context, processorManager1 *processor.ProcessorManager) {
	var (
		testPod0     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5)
		testPod1     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5)
		testPod2     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 17,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    24,
				Watermark: 17,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimeline().Put(watermark)
	}
	processorManager1.AddProcessor("testPod0", testPod0)
	processorManager1.AddProcessor("testPod1", testPod1)
	processorManager1.AddProcessor("testPod2", testPod2)
}

func getHeadWMBTest3(ctx context.Context, processorManager1 *processor.ProcessorManager) {
	var (
		testPod0     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5)
		testPod1     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5)
		testPod2     = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    26,
				Watermark: 17,
			},
		}
		pod2Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    24,
				Watermark: 17,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimeline().Put(watermark)
	}
	for _, watermark := range pod2Timeline {
		testPod2.GetOffsetTimeline().Put(watermark)
	}
	processorManager1.AddProcessor("testPod0", testPod0)
	processorManager1.AddProcessor("testPod1", testPod1)
	processorManager1.AddProcessor("testPod2", testPod2)
}

func getHeadWMBTest4(ctx context.Context, processorManager1 *processor.ProcessorManager) {
	var (
		testPod0 = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1"), 5)
		testPod1 = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod2"), 5)
		testPod2 = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod3"), 5)
	)
	processorManager1.AddProcessor("testPod0", testPod0)
	processorManager1.AddProcessor("testPod1", testPod1)
	processorManager1.AddProcessor("testPod2", testPod2)
}

func otValueToBytes(offset int64, watermark int64, idle bool) ([]byte, error) {
	otValue := wmb.WMB{
		Offset:    offset,
		Watermark: watermark,
		Idle:      idle,
	}
	otValueByte, err := otValue.EncodeToBytes()
	return otValueByte, err
}

// end to end test for fetcher using inmem store
func TestFetcherWithSameOTBucket_InMem(t *testing.T) {
	var (
		err          error
		pipelineName       = "testFetch"
		keyspace           = "fetcherTest"
		hbBucketName       = keyspace + "_PROCESSORS"
		otBucketName       = keyspace + "_OT"
		epoch        int64 = 1651161600000
		testOffset   int64 = 100
		wg           sync.WaitGroup
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	hbStore, hbWatcherCh, err := inmem.NewKVInMemKVStore(ctx, pipelineName, hbBucketName)
	assert.NoError(t, err)
	defer hbStore.Close()
	otStore, otWatcherCh, err := inmem.NewKVInMemKVStore(ctx, pipelineName, otBucketName)
	assert.NoError(t, err)
	defer otStore.Close()

	otValueByte, err := otValueToBytes(testOffset, epoch, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	epoch += 60000

	otValueByte, err = otValueToBytes(testOffset+5, epoch, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p2", otValueByte)
	assert.NoError(t, err)

	hbWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_PROCESSORS", hbWatcherCh)
	assert.NoError(t, err)
	otWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_OT", otWatcherCh)
	assert.NoError(t, err)
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	var processorManager = processor.NewProcessorManager(ctx, storeWatcher)
	var fetcher = NewEdgeFetcher(ctx, "testBuffer", storeWatcher, processorManager)
	// start p1 heartbeat for 3 loops
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 3; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
		err = hbStore.DeleteKey(ctx, "p1")
		assert.NoError(t, err)
	}()

	// start p2 heartbeat for 20 loops (20 seconds)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 20; i++ {
			err = hbStore.PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
	}()

	allProcessors := processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())

	// "p1" status becomes deleted after 3 loops
	for !allProcessors["p1"].IsDeleted() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be deleted: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	_ = fetcher.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }))
	allProcessors = processorManager.GetAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" should be deleted after this GetWatermark offset=101
	// because "p1" offsetTimeline's head offset=100, which is < inputOffset 103
	_ = fetcher.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+3, 10) }))
	allProcessors = processorManager.GetAllProcessors()
	assert.Equal(t, 1, len(allProcessors))
	assert.True(t, allProcessors["p2"].IsActive())

	time.Sleep(time.Second)
	// resume p1 after one second
	// run for 5 loops
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 5; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
	}()

	// wait until p1 becomes active
	time.Sleep(1 * time.Second)
	allProcessors = processorManager.GetAllProcessors()
	for !allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be active: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}
	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" has been deleted from vertex.Processors
	// so "p1" will be considered as a new processors with a new default offset timeline
	_ = fetcher.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+1, 10) }))
	p1 := processorManager.GetProcessor("p1")
	assert.NotNil(t, p1)
	assert.True(t, p1.IsActive())
	assert.NotNil(t, p1.GetOffsetTimeline())
	assert.Equal(t, int64(-1), p1.GetOffsetTimeline().GetHeadOffset())

	// publish a new watermark 101
	otValueByte, err = otValueToBytes(testOffset+1, epoch, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	// "p1" becomes inactive after 5 loops
	for !allProcessors["p1"].IsInactive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be inactive: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	time.Sleep(time.Second)

	// resume after one second
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 3; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
	}()

	allProcessors = processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	// added 101 in the previous steps for p1, so the head should be 101 after resume
	assert.Equal(t, int64(101), p1.GetOffsetTimeline().GetHeadOffset())
	wg.Wait()
	cancel()
}

// end to end test for fetcher with same ot bucket
func TestFetcherWithSameOTBucket(t *testing.T) {
	var (
		keyspace         = "fetcherTest"
		epoch      int64 = 1651161600000
		testOffset int64 = 100
		wg         sync.WaitGroup
	)

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// connect to NATS
	nc, err := natstest.JetStreamClient(t, s).Connect(context.TODO())
	assert.NoError(t, err)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create heartbeat bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_PROCESSORS",
		Description:  fmt.Sprintf("[%s] heartbeat bucket", keyspace),
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_PROCESSORS") }()
	assert.NoError(t, err)

	// create offset timeline bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_OT",
		Description:  "",
		MaxValueSize: 0,
		History:      2,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_OT") }()
	assert.NoError(t, err)

	defaultJetStreamClient := natstest.JetStreamClient(t, s)

	// create hbStore
	hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, "testFetch", keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	defer hbStore.Close()

	// create otStore
	otStore, err := jetstream.NewKVJetStreamKVStore(ctx, "testFetch", keyspace+"_OT", defaultJetStreamClient)
	assert.NoError(t, err)
	defer otStore.Close()

	// put values into otStore

	// this first entry should not be in the offset timeline because we set the wmb bucket history to 2
	otValueByte, err := otValueToBytes(testOffset, epoch+100, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	otValueByte, err = otValueToBytes(testOffset+1, epoch+200, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	otValueByte, err = otValueToBytes(testOffset+2, epoch+300, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	epoch += 60000

	otValueByte, err = otValueToBytes(testOffset+5, epoch+500, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p2", otValueByte)
	assert.NoError(t, err)

	// create watchers for heartbeat and offset timeline
	hbWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, "testFetch", keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	otWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, "testFetch", keyspace+"_OT", defaultJetStreamClient)
	assert.NoError(t, err)
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	processorManager := processor.NewProcessorManager(ctx, storeWatcher)
	fetcher := NewEdgeFetcher(ctx, "testBuffer", storeWatcher, processorManager)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 3; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
		err = hbStore.DeleteKey(ctx, "p1")
		assert.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		// run p2 for 20 seconds
		for i := 0; i < 20; i++ {
			err = hbStore.PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
	}()

	allProcessors := processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	for allProcessors["p1"].GetOffsetTimeline().Dump() != "[1651161600300:102] -> [1651161600200:101] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [1651161600300:102] -> [1651161600200:101] -> [-1:-1]..., got %s: %s", allProcessors["p1"].GetOffsetTimeline().Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())

	// "p1" is deleted after 5 loops
	for !allProcessors["p1"].IsDeleted() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be deleted: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	allProcessors = processorManager.GetAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())

	_ = fetcher.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }))
	allProcessors = processorManager.GetAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" should be deleted after this GetWatermark offset=101
	// because "p1" offsetTimeline's head offset=102, which is < inputOffset 103
	_ = fetcher.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+3, 10) }))
	allProcessors = processorManager.GetAllProcessors()
	assert.Equal(t, 1, len(allProcessors))
	assert.True(t, allProcessors["p2"].IsActive())

	time.Sleep(time.Second)
	// resume after one second
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 5; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
	}()

	// wait until p1 becomes active
	time.Sleep(1 * time.Second)
	allProcessors = processorManager.GetAllProcessors()
	for !allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be active: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" has been deleted from vertex.Processors
	// so "p1" will be considered as a new processors with a new default offset timeline
	_ = fetcher.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+1, 10) }))
	p1 := processorManager.GetProcessor("p1")
	assert.NotNil(t, p1)
	assert.True(t, p1.IsActive())
	assert.NotNil(t, p1.GetOffsetTimeline())
	assert.Equal(t, int64(-1), p1.GetOffsetTimeline().GetHeadOffset())

	// publish a new watermark 103
	otValueByte, err = otValueToBytes(testOffset+3, epoch+500, false)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	// "p1" becomes inactive after 5 loops
	for !allProcessors["p1"].IsInactive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be inactive: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	time.Sleep(time.Second)

	// resume after one second
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 10; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
	}()

	allProcessors = processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	// added 103 in the previous steps for p1, so the head should be 103 after resume
	assert.Equal(t, int64(103), p1.GetOffsetTimeline().GetHeadOffset())

	for allProcessors["p1"].GetOffsetTimeline().Dump() != "[1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1]..., got %s: %s", allProcessors["p1"].GetOffsetTimeline().Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	// publish an idle watermark: simulate reduce
	otValueByte, err = otValueToBytes(106, epoch+600, true)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	// p1 should get the head offset watermark from p2
	for allProcessors["p1"].GetOffsetTimeline().Dump() != "[IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1]..., got %s: %s", allProcessors["p1"].GetOffsetTimeline().Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	// publish an idle watermark: simulate map
	otValueByte, err = otValueToBytes(107, epoch+700, true)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	// p1 should get the head offset watermark from p2
	for allProcessors["p1"].GetOffsetTimeline().Dump() != "[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("[IDLE 1651161660700:107] -> [IDLE 1651161660600:106] -> [1651161660500:103] -> ..., got %s: %s", allProcessors["p1"].GetOffsetTimeline().Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.GetAllProcessors()
		}
	}

	wg.Wait()
	cancel()
}
