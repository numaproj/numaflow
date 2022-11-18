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

	"github.com/numaproj/numaflow/pkg/watermark/ot"
	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

func otValueToBytes(offset int64, watermark int64) ([]byte, error) {
	otValue := ot.Value{
		Offset:    offset,
		Watermark: watermark,
	}
	otValueByte, err := otValue.EncodeToBytes()
	return otValueByte, err
}

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

	otValueByte, err := otValueToBytes(testOffset, epoch)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	epoch += 60000

	otValueByte, err = otValueToBytes(testOffset+5, epoch)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p2", otValueByte)
	assert.NoError(t, err)

	hbWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_PROCESSORS", hbWatcherCh)
	assert.NoError(t, err)
	otWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_OT", otWatcherCh)
	assert.NoError(t, err)
	var testBuffer = NewEdgeFetcher(ctx, "testBuffer", store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)).(*edgeFetcher)

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

	allProcessors := testBuffer.processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
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
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}

	allProcessors = testBuffer.processorManager.GetAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())

	_ = testBuffer.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset, 10) }))
	allProcessors = testBuffer.processorManager.GetAllProcessors()
	assert.Equal(t, 2, len(allProcessors))
	assert.True(t, allProcessors["p1"].IsDeleted())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" should be deleted after this GetWatermark offset=101
	// because "p1" offsetTimeline's head offset=100, which is < inputOffset 103
	_ = testBuffer.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+3, 10) }))
	allProcessors = testBuffer.processorManager.GetAllProcessors()
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
	allProcessors = testBuffer.processorManager.GetAllProcessors()
	for !allProcessors["p1"].IsActive() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 to be active: %s", ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}
	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())
	// "p1" has been deleted from vertex.Processors
	// so "p1" will be considered as a new processors with a new default offset timeline
	_ = testBuffer.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+1, 10) }))
	p1 := testBuffer.processorManager.GetProcessor("p1")
	assert.NotNil(t, p1)
	assert.True(t, p1.IsActive())
	assert.NotNil(t, p1.offsetTimeline)
	assert.Equal(t, int64(-1), p1.offsetTimeline.GetHeadOffset())

	// publish a new watermark 101
	otValueByte, err = otValueToBytes(testOffset+1, epoch)
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
			allProcessors = testBuffer.processorManager.GetAllProcessors()
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

	allProcessors = testBuffer.processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}

	// added 101 in the previous steps for p1, so the head should be 101 after resume
	assert.Equal(t, int64(101), p1.offsetTimeline.GetHeadOffset())
	wg.Wait()
	cancel()
}
