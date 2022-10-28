//go:build isb_jetstream

package fetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
)

func TestFetcherWithSameOTBucket(t *testing.T) {
	var (
		keyspace         = "fetcherTest"
		epoch      int64 = 1651161600000
		testOffset int64 = 100
		wg         sync.WaitGroup
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// connect to NATS
	nc, err := jsclient.NewDefaultJetStreamClient(nats.DefaultURL).Connect(context.TODO())
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
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_OT") }()
	assert.NoError(t, err)

	// create default JetStream client
	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(nats.DefaultURL)

	// create hbStore
	hbStore, err := jetstream.NewKVJetStreamKVStore(ctx, "testFetch", keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	defer hbStore.Close()

	// create otStore
	otStore, err := jetstream.NewKVJetStreamKVStore(ctx, "testFetch", keyspace+"_OT", defaultJetStreamClient)
	assert.NoError(t, err)
	defer otStore.Close()

	// put values into otStore
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(testOffset))
	err = otStore.PutKV(ctx, fmt.Sprintf("%s%s%d", "p1", "_", epoch), b)
	assert.NoError(t, err)
	epoch += 60000
	binary.LittleEndian.PutUint64(b, uint64(testOffset+5))
	err = otStore.PutKV(ctx, fmt.Sprintf("%s%s%d", "p2", "_", epoch), b)
	assert.NoError(t, err)

	// create watchers for heartbeat and offset timeline
	hbWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, "testFetch", keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	otWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, "testFetch", keyspace+"_OT", defaultJetStreamClient)
	assert.NoError(t, err)
	var testVertex = NewProcessorManager(ctx, store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher), WithPodHeartbeatRate(1), WithRefreshingProcessorsRate(1), WithSeparateOTBuckets(false))
	var testBuffer = NewEdgeFetcher(ctx, "testBuffer", testVertex).(*edgeFetcher)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 3; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
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

	// "p1" is deleted after 5 loops
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
	// resume after one second
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 5; i++ {
			err = hbStore.PutKV(ctx, "p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
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
	// because the bucket hasn't been cleaned up, the new watcher will read all the history data to create this new offsetTimeline
	assert.Equal(t, int64(100), p1.offsetTimeline.GetHeadOffset())

	// publish a new watermark 101
	binary.LittleEndian.PutUint64(b, uint64(testOffset+1))
	err = otStore.PutKV(ctx, fmt.Sprintf("%s%s%d", "p1", "_", epoch), b)
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
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
		}
	}()

	allProcessors = testBuffer.processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
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

func TestFetcherWithSeparateOTBucket(t *testing.T) {
	// FIXME: Check for seperate buckets implementation for single watcher or multiple watcher
	//   Maybe we should not support seperate OT because controller does not support it
	t.Skip()
	// uncomment to debug
	// os.Setenv("NUMAFLOW_DEBUG", "true")

	var ctx = context.Background()

	// Connect to NATS
	nc, err := jsclient.NewDefaultJetStreamClient(nats.DefaultURL).Connect(context.TODO())
	assert.Nil(t, err)

	// Create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	assert.Nil(t, err)

	// create heartbeat bucket
	var keyspace = "fetcherTest"

	heartbeatBucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
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
	assert.Nil(t, err)

	var epoch int64 = 1651161600000
	var testOffset int64 = 100
	p1OT, _ := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_OT_" + "p1",
		Description:  "",
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_OT_" + "p1") }()
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(testOffset))
	_, err = p1OT.Put(fmt.Sprintf("%d", epoch), b)
	assert.NoError(t, err)

	epoch += 60000
	binary.LittleEndian.PutUint64(b, uint64(testOffset+5))
	p2OT, _ := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_OT_" + "p2",
		Description:  "",
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_OT_" + "p2") }()
	_, err = p2OT.Put(fmt.Sprintf("%d", epoch), b)
	assert.NoError(t, err)

	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(nats.DefaultURL)

	hbWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, "testFetch", keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	otWatcher, err := jetstream.NewKVJetStreamKVWatch(ctx, "testFetch", keyspace+"_OT", defaultJetStreamClient)
	assert.NoError(t, err)
	var testVertex = NewProcessorManager(ctx, store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher), WithPodHeartbeatRate(1), WithRefreshingProcessorsRate(1), WithSeparateOTBuckets(true))
	var testBuffer = NewEdgeFetcher(ctx, "testBuffer", testVertex).(*edgeFetcher)

	// var location, _ = time.LoadLocation("UTC")
	go func() {
		var err error
		for i := 0; i < 3; i++ {
			_, err = heartbeatBucket.Put("p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
		}
		err = heartbeatBucket.Delete("p1")
		assert.NoError(t, err)
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_, err := heartbeatBucket.Put("p2", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	allProcessors := testBuffer.processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}

	assert.True(t, allProcessors["p1"].IsActive())
	assert.True(t, allProcessors["p2"].IsActive())

	// "p1" is deleted after 5 loops
	for !allProcessors["p1"].IsDeleted() {
		select {
		case <-ctx.Done():
			t.Fatalf("expected p1 to be deleted: %s", ctx.Err())
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

	// resume after one second
	go func() {
		var err error
		for i := 0; i < 5; i++ {
			_, err = heartbeatBucket.Put("p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
		}
	}()

	// wait until p1 becomes active
	allProcessors = testBuffer.processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
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
	newP1 := testBuffer.processorManager.GetProcessor("p1")
	assert.NotNil(t, newP1)
	assert.True(t, newP1.IsActive())
	assert.NotNil(t, newP1.offsetTimeline)
	// because the bucket hasn't been cleaned up, the new watcher will read all the history data to create this new offsetTimeline
	assert.Equal(t, int64(100), newP1.offsetTimeline.GetHeadOffset())

	// publish a new watermark 101
	binary.LittleEndian.PutUint64(b, uint64(testOffset+1))
	_, err = p1OT.Put(fmt.Sprintf("%d", epoch), b)
	assert.NoError(t, err)

	// "p1" becomes inactive after 5 loops
	for !allProcessors["p1"].IsInactive() {
		select {
		case <-ctx.Done():
			t.Fatalf("expected p1 to be inactive: %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}

	time.Sleep(time.Second)

	// resume after one second
	go func() {
		var err error
		for i := 0; i < 5; i++ {
			_, err = heartbeatBucket.Put("p1", []byte(fmt.Sprintf("%d", time.Now().Unix())))
			assert.NoError(t, err)
			time.Sleep(time.Duration(testVertex.opts.podHeartbeatRate) * time.Second)
		}
	}()

	allProcessors = testBuffer.processorManager.GetAllProcessors()
	for len(allProcessors) != 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("expected 2 processors, got %d: %s", len(allProcessors), ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}

	// added 101 in the previous steps for newP1, so the head should be 101 after resume
	assert.Equal(t, int64(101), newP1.offsetTimeline.GetHeadOffset())

}
