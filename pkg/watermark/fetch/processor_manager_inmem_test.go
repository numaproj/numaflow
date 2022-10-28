package fetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

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
	hb, hbWatcherCh, err := inmem.NewKVInMemKVStore(ctx, pipelineName, hbBucketName)
	assert.NoError(t, err)
	defer hb.Close()
	ot, otWatcherCh, err := inmem.NewKVInMemKVStore(ctx, pipelineName, otBucketName)
	assert.NoError(t, err)
	defer ot.Close()

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(testOffset))
	// this key format is meant for non-separate OT watcher
	err = ot.PutKV(ctx, fmt.Sprintf("%s%s%d", "p1", "_", epoch), b)
	assert.NoError(t, err)

	epoch += 60000
	binary.LittleEndian.PutUint64(b, uint64(testOffset+5))
	// this key format is meant for non-separate OT watcher
	err = ot.PutKV(ctx, fmt.Sprintf("%s%s%d", "p2", "_", epoch), b)
	assert.NoError(t, err)

	hbWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_PROCESSORS", hbWatcherCh)
	assert.NoError(t, err)
	otWatcher, err := inmem.NewInMemWatch(ctx, "testFetch", keyspace+"_OT", otWatcherCh)
	assert.NoError(t, err)
	var testVertex = NewProcessorManager(ctx, store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher), WithPodHeartbeatRate(1), WithRefreshingProcessorsRate(1), WithSeparateOTBuckets(false))
	var testBuffer = NewEdgeFetcher(ctx, "testBuffer", testVertex).(*edgeFetcher)

	// start p1 heartbeat for 3 loops
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 3; i++ {
			err = hb.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
		}
		err = hb.DeleteKey(ctx, "p1")
		assert.NoError(t, err)
	}()

	// start p2 heartbeat for 20 loops (20 seconds)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 20; i++ {
			err = hb.PutKV(ctx, "p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
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
			err = hb.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
		}
	}()

	// wait until p1 becomes active
	time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
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
	// so "p1" will be considered as a new processors and a new offsetTimeline watcher for "p1" will be created
	_ = testBuffer.GetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(testOffset+1, 10) }))
	p1 := testBuffer.processorManager.GetProcessor("p1")

	assert.NotNil(t, p1)
	assert.True(t, p1.IsActive())
	assert.NotNil(t, p1.offsetTimeline)
	// wait till the offsetTimeline has been populated
	newP1head := p1.offsetTimeline.GetHeadOffset()
	for newP1head == -1 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected head offset to not be equal to -1, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
			newP1head = p1.offsetTimeline.GetHeadOffset()
		}
	}
	// because we keep the kv updates history in the in mem watermark
	// the new watcher will read all the history data to create this new offsetTimeline
	assert.Equal(t, int64(100), p1.offsetTimeline.GetHeadOffset())

	// publish a new watermark 101
	binary.LittleEndian.PutUint64(b, uint64(testOffset+1))
	err = ot.PutKV(ctx, fmt.Sprintf("%s%s%d", "p1", "_", epoch), b)
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
			err = hb.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
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
