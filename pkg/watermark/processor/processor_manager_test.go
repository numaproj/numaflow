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

package processor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

func otValueToBytes(offset int64, watermark int64, idle bool, partition int32) ([]byte, error) {
	otValue := wmb.WMB{
		Offset:    offset,
		Watermark: watermark,
		Idle:      idle,
		Partition: partition,
	}
	otValueByte, err := otValue.EncodeToBytes()
	return otValueByte, err
}

func TestProcessorManager(t *testing.T) {
	var (
		err          error
		pipelineName = "testFetch"
		keyspace     = "fetcherTest"
		hbBucketName = keyspace + "_PROCESSORS"
		otBucketName = keyspace + "_OT"
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

	hbWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_PROCESSORS", hbWatcherCh)
	assert.NoError(t, err)
	otWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_OT", otWatcherCh)
	assert.NoError(t, err)
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	var processorManager = NewProcessorManager(ctx, storeWatcher)
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

	allProcessors = processorManager.GetAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())

	processorManager.DeleteProcessor("p1")
	processorManager.DeleteProcessor("p2")
	assert.Equal(t, 0, len(processorManager.GetAllProcessors()))
	cancel()
}

func TestProcessorManagerWatchForMap(t *testing.T) {
	var (
		err          error
		pipelineName       = "testFetch"
		keyspace           = "fetcherTest"
		hbBucketName       = keyspace + "_PROCESSORS"
		otBucketName       = keyspace + "_OT"
		epoch        int64 = 60000
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

	hbWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_PROCESSORS", hbWatcherCh)
	assert.NoError(t, err)
	otWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_OT", otWatcherCh)
	assert.NoError(t, err)
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	var processorManager = NewProcessorManager(ctx, storeWatcher)
	// start p1 heartbeat for 3 loops
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
				time.Sleep(1 * time.Second)
			}
		}
	}(ctx)

	// start p2 heartbeat for 20 loops (20 seconds)
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = hbStore.PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
				time.Sleep(1 * time.Second)
			}
		}
	}(ctx)

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

	for i := 0; i < 3; i++ {
		epoch += 1000
		testOffset += 5
		otValueByte, _ := otValueToBytes(testOffset, epoch, false, 0)
		err = otStore.PutKV(ctx, "p2", otValueByte)
		assert.NoError(t, err)
		err = otStore.PutKV(ctx, "p1", otValueByte)
		assert.NoError(t, err)
	}
	for processorManager.GetProcessor("p1").GetOffsetTimeline().GetHeadOffset() == -1 || processorManager.GetProcessor("p2").GetOffsetTimeline().GetHeadOffset() == -1 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected offset timeline to be updated: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    115,
		Watermark: 63000,
		Partition: 0,
	}, processorManager.GetProcessor("p1").GetOffsetTimeline().GetHeadWMB())
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    115,
		Watermark: 63000,
		Partition: 0,
	}, processorManager.GetProcessor("p2").GetOffsetTimeline().GetHeadWMB())
	processorManager.DeleteProcessor("p1")
	processorManager.DeleteProcessor("p2")
	cancel()
}

func TestProcessorManagerWatchForReduce(t *testing.T) {
	var (
		err          error
		pipelineName       = "testFetch"
		keyspace           = "fetcherTest"
		hbBucketName       = keyspace + "_PROCESSORS"
		otBucketName       = keyspace + "_OT"
		epoch        int64 = 60000
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

	hbWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_PROCESSORS", hbWatcherCh)
	assert.NoError(t, err)
	otWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_OT", otWatcherCh)
	assert.NoError(t, err)
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	var processorManager = NewProcessorManager(ctx, storeWatcher, WithIsReduce(true), WithVertexReplica(2))
	// start p1 heartbeat for 3 loops
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
				time.Sleep(1 * time.Second)
			}
		}
	}(ctx)

	// start p2 heartbeat for 20 loops (20 seconds)
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = hbStore.PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
				time.Sleep(1 * time.Second)
			}
		}
	}(ctx)

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

	for i := 0; i < 3; i++ {
		epoch += 1000
		testOffset += 5
		otValueByte, _ := otValueToBytes(testOffset, epoch, false, 2)
		err = otStore.PutKV(ctx, "p2", otValueByte)
		assert.NoError(t, err)
		err = otStore.PutKV(ctx, "p1", otValueByte)
		assert.NoError(t, err)
	}

	// send data from different partition, should not be processed
	for i := 0; i < 3; i++ {
		epoch += 1000
		testOffset += 5
		otValueByte, _ := otValueToBytes(testOffset, epoch, false, 3)
		err = otStore.PutKV(ctx, "p2", otValueByte)
		assert.NoError(t, err)
		err = otStore.PutKV(ctx, "p1", otValueByte)
		assert.NoError(t, err)
	}
	for processorManager.GetProcessor("p1").GetOffsetTimeline().GetHeadOffset() == -1 || processorManager.GetProcessor("p2").GetOffsetTimeline().GetHeadOffset() == -1 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected offset timeline to be updated: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
	// since the vertex replica is 2, only the wmb with partition 2 should be considered
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    115,
		Watermark: 63000,
		Partition: 2,
	}, processorManager.GetProcessor("p1").GetOffsetTimeline().GetHeadWMB())
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    115,
		Watermark: 63000,
		Partition: 2,
	}, processorManager.GetProcessor("p2").GetOffsetTimeline().GetHeadWMB())
	processorManager.DeleteProcessor("p1")
	processorManager.DeleteProcessor("p2")
	cancel()
}
