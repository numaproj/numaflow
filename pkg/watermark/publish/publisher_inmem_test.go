package publish

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/store/inmem"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

func TestPublisherWithSharedOTBuckets_InMem(t *testing.T) {
	var ctx = context.Background()

	var publisherHBKeyspace = "publisherTest_PROCESSORS"

	// this test uses separate OT buckets, so it is an OT bucket per processor
	var publisherOTKeyspace = "publisherTest_OT_publisherTestPod1"

	heartbeatKV, _, err := inmem.NewKVInMemKVStore(ctx, "testPublisher", publisherHBKeyspace)
	assert.NoError(t, err)
	otKV, _, err := inmem.NewKVInMemKVStore(ctx, "testPublisher", publisherOTKeyspace)
	assert.NoError(t, err)

	publishEntity := processor.NewProcessorEntity("publisherTestPod1")

	p := NewPublish(ctx, publishEntity, store.BuildWatermarkStore(heartbeatKV, otKV), WithAutoRefreshHeartbeatDisabled(), WithPodHeartbeatRate(1)).(*publish)

	var epoch int64 = 1651161600000
	var location, _ = time.LoadLocation("UTC")
	for i := 0; i < 3; i++ {
		p.PublishWatermark(processor.Watermark(time.UnixMilli(epoch).In(location)), isb.SimpleStringOffset(func() string { return strconv.Itoa(i) }))
		epoch += 60000
		time.Sleep(time.Millisecond)
	}
	// publish a stale watermark (offset doesn't matter)
	p.PublishWatermark(processor.Watermark(time.UnixMilli(epoch-120000).In(location)), isb.SimpleStringOffset(func() string { return strconv.Itoa(0) }))

	keys := p.getAllOTKeysFromBucket()
	assert.Equal(t, []string{"publisherTestPod1_1651161600000", "publisherTestPod1_1651161660000", "publisherTestPod1_1651161720000"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), head.String())

	p.StopPublisher()

	_, err = p.heartbeatStore.GetValue(ctx, publishEntity.GetID())
	assert.Equal(t, fmt.Errorf("key publisherTestPod1 not found"), err)

}

func TestPublisherWithSeparateOTBucket_InMem(t *testing.T) {
	var ctx = context.Background()

	var keyspace = "publisherTest"

	publishEntity := processor.NewProcessorEntity("publisherTestPod1", processor.WithSeparateOTBuckets(true))
	heartbeatKV, _, err := inmem.NewKVInMemKVStore(ctx, "testPublisher", keyspace+"_PROCESSORS")
	assert.NoError(t, err)
	otKV, _, err := inmem.NewKVInMemKVStore(ctx, "testPublisher", keyspace+"_OT")
	assert.NoError(t, err)

	p := NewPublish(ctx, publishEntity, store.BuildWatermarkStore(heartbeatKV, otKV), WithAutoRefreshHeartbeatDisabled(), WithPodHeartbeatRate(1)).(*publish)

	var epoch int64 = 1651161600000
	var location, _ = time.LoadLocation("UTC")
	for i := 0; i < 3; i++ {
		p.PublishWatermark(processor.Watermark(time.UnixMilli(epoch).In(location)), isb.SimpleStringOffset(func() string { return strconv.Itoa(i) }))
		epoch += 60000
		time.Sleep(time.Millisecond)
	}
	// publish a stale watermark (offset doesn't matter)
	p.PublishWatermark(processor.Watermark(time.UnixMilli(epoch-120000).In(location)), isb.SimpleStringOffset(func() string { return strconv.Itoa(0) }))

	keys := p.getAllOTKeysFromBucket()
	assert.Equal(t, []string{"1651161600000", "1651161660000", "1651161720000"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), head.String())

	p.StopPublisher()

	_, err = p.heartbeatStore.GetValue(ctx, publishEntity.GetID())
	assert.ErrorContains(t, err, "key publisherTestPod1 not found")
}
