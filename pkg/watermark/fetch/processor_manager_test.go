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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/numaproj/numaflow/pkg/watermark/store"
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

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestProcessorManager(t *testing.T) {
	var (
		err      error
		keyspace = "fetcherTest"
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	wmStore, err := store.BuildInmemWatermarkStore(ctx, keyspace)
	assert.NoError(t, err)

	assert.NoError(t, err)
	var processorManager = newProcessorManager(ctx, wmStore, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	// start p1 heartbeat for 3 loops then delete p1
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 3; i++ {
			err = wmStore.HeartbeatStore().PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}
		err = wmStore.HeartbeatStore().DeleteKey(ctx, "p1")
		assert.NoError(t, err)
	}()

	wg.Add(1)
	// start p2 heartbeat.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.After(1 * time.Second):
				err := wmStore.HeartbeatStore().PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
			case <-ctx.Done():
				return
			}
		}
	}()

	allProcessors := processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.getAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())

	// "p1" status becomes deleted after 3 loops
	for !allProcessors["p1"].IsDeleted() {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected p1 to be deleted: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.getAllProcessors()
		}
	}

	allProcessors = processorManager.getAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())

	processorManager.deleteProcessor("p1")
	processorManager.deleteProcessor("p2")
	assert.Equal(t, 0, len(processorManager.getAllProcessors()))
	cancel()
	wg.Wait()
	_ = wmStore.Close()
}

func TestProcessorManagerWatchForMapWithOnePartition(t *testing.T) {
	var (
		err        error
		keyspace         = "fetcherTest"
		epoch      int64 = 60000
		testOffset int64 = 100
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	wmStore, _ := store.BuildInmemWatermarkStore(ctx, keyspace)
	assert.NoError(t, err)

	var processorManager = newProcessorManager(ctx, wmStore, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	// start p1 heartbeat for 3 loops
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-time.After(1 * time.Second):
				err := wmStore.HeartbeatStore().PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	wg.Add(1)
	// start p2 heartbeat
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-time.After(1 * time.Second):
				err := wmStore.HeartbeatStore().PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	allProcessors := processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.getAllProcessors()
		}
	}

	for i := 0; i < 3; i++ {
		epoch += 1000
		testOffset += 5
		otValueByte, _ := otValueToBytes(testOffset, epoch, false, 0)
		err = wmStore.OffsetTimelineStore().PutKV(ctx, "p2", otValueByte)
		assert.NoError(t, err)
		err = wmStore.OffsetTimelineStore().PutKV(ctx, "p1", otValueByte)
		assert.NoError(t, err)
	}
	for processorManager.getProcessor("p1").GetOffsetTimelines()[0].GetHeadOffset() != 115 || processorManager.getProcessor("p2").GetOffsetTimelines()[0].GetHeadOffset() != 115 {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
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
	}, processorManager.getProcessor("p1").GetOffsetTimelines()[0].GetHeadWMB())
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    115,
		Watermark: 63000,
		Partition: 0,
	}, processorManager.getProcessor("p2").GetOffsetTimelines()[0].GetHeadWMB())
	processorManager.deleteProcessor("p1")
	processorManager.deleteProcessor("p2")
	cancel()
	wg.Wait()
	_ = wmStore.Close()
}

func TestProcessorManagerWatchForReduce(t *testing.T) {
	var (
		err        error
		keyspace         = "fetcherTest"
		epoch      int64 = 60000
		testOffset int64 = 100
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	wmStore, _ := store.BuildInmemWatermarkStore(ctx, keyspace)
	assert.NoError(t, err)

	var processorManager = newProcessorManager(ctx, wmStore, 1, WithIsReduce(true), WithVertexReplica(2))
	var wg sync.WaitGroup

	wg.Add(1)
	// start p1 heartbeat for 3 loops
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-time.After(1 * time.Second):
				err := wmStore.HeartbeatStore().PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	wg.Add(1)
	// start p2 heartbeat
	go func(ctx context.Context) {
		defer wg.Done()
		var err error
		for {
			select {
			case <-time.After(1 * time.Second):
				err = wmStore.HeartbeatStore().PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	allProcessors := processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.getAllProcessors()
		}
	}

	for i := 0; i < 3; i++ {
		epoch += 1000
		testOffset += 5
		otValueByte, _ := otValueToBytes(testOffset, epoch, false, 2)
		err = wmStore.OffsetTimelineStore().PutKV(ctx, "p2", otValueByte)
		assert.NoError(t, err)
		err = wmStore.OffsetTimelineStore().PutKV(ctx, "p1", otValueByte)
		assert.NoError(t, err)
	}

	// send data from different partition, should not be processed
	for i := 0; i < 3; i++ {
		epoch += 1000
		testOffset += 5
		otValueByte, _ := otValueToBytes(testOffset, epoch, false, 3)
		err = wmStore.OffsetTimelineStore().PutKV(ctx, "p2", otValueByte)
		assert.NoError(t, err)
		err = wmStore.OffsetTimelineStore().PutKV(ctx, "p1", otValueByte)
		assert.NoError(t, err)
	}
	for processorManager.getProcessor("p1").GetOffsetTimelines()[0].GetHeadOffset() != 115 || processorManager.getProcessor("p2").GetOffsetTimelines()[0].GetHeadOffset() != 115 {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
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
	}, processorManager.getProcessor("p1").GetOffsetTimelines()[0].GetHeadWMB())
	assert.Equal(t, wmb.WMB{
		Idle:      false,
		Offset:    115,
		Watermark: 63000,
		Partition: 2,
	}, processorManager.getProcessor("p2").GetOffsetTimelines()[0].GetHeadWMB())
	processorManager.deleteProcessor("p1")
	processorManager.deleteProcessor("p2")
	cancel()
	wg.Wait()
	_ = wmStore.Close()
}

func TestProcessorManagerWatchForMapWithMultiplePartition(t *testing.T) {
	var (
		err            error
		keyspace             = "fetcherTest"
		epoch          int64 = 60000
		testOffset     int64 = 100
		partitionCount       = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	wmStore, err := store.BuildInmemWatermarkStore(ctx, keyspace)
	assert.NoError(t, err)

	var processorManager = newProcessorManager(ctx, wmStore, 3)
	var wg sync.WaitGroup

	wg.Add(1)
	// start p1 heartbeat for 3 loops
	go func(ctx context.Context) {
		defer wg.Done()
		var err error
		for {
			select {
			case <-time.After(1 * time.Second):
				err = wmStore.HeartbeatStore().PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	// start p2 heartbeat.
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		var err error
		for {
			select {
			case <-time.After(1 * time.Second):
				err = wmStore.HeartbeatStore().PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
				assert.NoError(t, err)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	allProcessors := processorManager.getAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = processorManager.getAllProcessors()
		}
	}

	for i := 0; i < 3; i++ {
		epoch += 1000
		testOffset += 5
		for j := 0; j < partitionCount; j++ {
			otValueByte, _ := otValueToBytes(testOffset, epoch, false, int32(j))
			err = wmStore.OffsetTimelineStore().PutKV(ctx, "p2", otValueByte)
			assert.NoError(t, err)
			err = wmStore.OffsetTimelineStore().PutKV(ctx, "p1", otValueByte)
			assert.NoError(t, err)
		}
	}
loop:
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("expected offset timeline to be updated: %s", ctx.Err())
			}
		default:
			for _, p := range processorManager.getAllProcessors() {
				for _, ot := range p.GetOffsetTimelines() {
					if ot.GetHeadWMB().Offset != 115 {
						goto notDone
					}
				}
				goto done
			}
		notDone:
			time.Sleep(1 * time.Millisecond)
		done:
			break loop
		}
	}
	for i := 0; i < partitionCount; i++ {
		assert.Equal(t, wmb.WMB{
			Idle:      false,
			Offset:    115,
			Watermark: 63000,
			Partition: int32(i),
		}, processorManager.getProcessor("p1").GetOffsetTimelines()[i].GetHeadWMB())
		assert.Equal(t, wmb.WMB{
			Idle:      false,
			Offset:    115,
			Watermark: 63000,
			Partition: int32(i),
		}, processorManager.getProcessor("p2").GetOffsetTimelines()[i].GetHeadWMB())
	}
	processorManager.deleteProcessor("p1")
	processorManager.deleteProcessor("p2")
	cancel()
	wg.Wait()
	_ = wmStore.Close()
}
