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

	"github.com/numaproj/numaflow/pkg/isb"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
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

	// this first entry should not be in the offset timeline because we set the ot bucket history to 2
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
	var testBuffer = NewEdgeFetcher(ctx, "testBuffer", store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)).(*edgeFetcher)

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

	for allProcessors["p1"].offsetTimeline.Dump() != "[1651161600300:102] -> [1651161600200:101] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [1651161600300:102] -> [1651161600200:101] -> [-1:-1]..., got %s: %s", allProcessors["p1"].offsetTimeline.Dump(), ctx.Err())
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
	// because "p1" offsetTimeline's head offset=102, which is < inputOffset 103
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
			allProcessors = testBuffer.processorManager.GetAllProcessors()
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

	// added 103 in the previous steps for p1, so the head should be 103 after resume
	assert.Equal(t, int64(103), p1.offsetTimeline.GetHeadOffset())

	for allProcessors["p1"].offsetTimeline.Dump() != "[1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [1651161660500:103] -> [-1:-1] -> [-1:-1] -> [-1:-1]..., got %s: %s", allProcessors["p1"].offsetTimeline.Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}

	// publish an idle watermark
	otValueByte, err = otValueToBytes(0, epoch+600, true)
	assert.NoError(t, err)
	err = otStore.PutKV(ctx, "p1", otValueByte)
	assert.NoError(t, err)

	// p1 should get the head offset watermark from p2
	for allProcessors["p1"].offsetTimeline.Dump() != "[1651161660500:105] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1] -> [-1:-1]" {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("expected p1 has the offset timeline [1651161660500:105] -> [-1:-1] -> [-1:-1] -> [-1:-1]..., got %s: %s", allProcessors["p1"].offsetTimeline.Dump(), ctx.Err())
			}
		default:
			time.Sleep(1 * time.Millisecond)
			allProcessors = testBuffer.processorManager.GetAllProcessors()
		}
	}

	wg.Wait()
	cancel()
}
