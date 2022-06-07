//go:build isb_jetstream

package publish

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"github.com/stretchr/testify/assert"
)

func createAndLaterDeleteBucket(js nats.JetStreamContext, kvConfig *nats.KeyValueConfig) (func(), error) {
	kv, err := js.CreateKeyValue(kvConfig)
	if err != nil {
		return nil, err
	}
	return func() {
		// ignore error as this will be executed only via defer
		_ = js.DeleteKeyValue(kv.Bucket())
		return
	}, nil
}

func TestPublisherWithSeparateOTBuckets(t *testing.T) {
	var ctx = context.Background()

	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	assert.Nil(t, err)

	// Create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	assert.Nil(t, err)

	var publisherHBKeyspace = "publisherTest_PROCESSORS"
	deleteFn, err := createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: publisherHBKeyspace})
	assert.NoError(t, err)
	defer deleteFn()

	// this test uses separate OT buckets, so it is an OT bucket per processor
	var publisherOTKeyspace = "publisherTest_OT_publisherTestPod1"
	deleteFn, err = createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: publisherOTKeyspace})
	assert.NoError(t, err)
	defer deleteFn()

	kvJetStreamSvc, err := jetstream.NewKVJetStreamSvc(ctx, publisherHBKeyspace, "testPublisher", js)
	assert.NoError(t, err)

	publishEntity := processor.NewProcessorEntity("publisherTestPod1", publisherHBKeyspace)

	p := NewPublish(ctx, publishEntity, kvJetStreamSvc, nil, nil, WithAutoRefreshHeartbeatDisabled(), WithPodHeartbeatRate(1))

	var epoch int64 = 1651161600
	var location, _ = time.LoadLocation("UTC")
	for i := 0; i < 3; i++ {
		p.PublishWatermark(processor.Watermark(time.Unix(epoch, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(i) }))
		epoch += 60
		time.Sleep(time.Millisecond)
	}
	// publish a stale watermark (offset doesn't matter)
	p.PublishWatermark(processor.Watermark(time.Unix(epoch-120, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(0) }))

	keys := p.getAllKeysFromBucket()
	assert.Equal(t, []string{"publisherTestPod1_1651161600", "publisherTestPod1_1651161660", "publisherTestPod1_1651161720"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), head.String())

	p.StopPublisher()

	_, err = js.KeyValue(publishEntity.GetBucketName())
	assert.Equal(t, nats.ErrBucketNotFound, err)

	_, err = p.publishStore.GetValue(ctx, publishEntity.GetID())
	assert.Equal(t, nats.ErrKeyNotFound, err)

}

func TestPublisherWithSharedOTBucket(t *testing.T) {
	var ctx = context.Background()

	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	assert.Nil(t, err)

	// Create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	assert.Nil(t, err)

	var keyspace = "publisherTest"

	deleteFn, err := createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: keyspace + "_PROCESSORS"})
	defer deleteFn()

	deleteFn, err = createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: keyspace + "_OT"})
	defer deleteFn()

	publishEntity := processor.NewProcessorEntity("publisherTestPod1", keyspace, processor.WithSeparateOTBuckets(true))

	kvJetStreamSvc, err := jetstream.NewKVJetStreamSvc(ctx, keyspace+"_PROCESSORS", "TestPublisherWithSharedOTBucket", js)
	assert.NoError(t, err)

	p := NewPublish(ctx, publishEntity, kvJetStreamSvc, nil, nil, WithAutoRefreshHeartbeatDisabled(), WithPodHeartbeatRate(1))

	var epoch int64 = 1651161600
	var location, _ = time.LoadLocation("UTC")
	for i := 0; i < 3; i++ {
		p.PublishWatermark(processor.Watermark(time.Unix(epoch, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(i) }))
		epoch += 60
		time.Sleep(time.Millisecond)
	}
	// publish a stale watermark (offset doesn't matter)
	p.PublishWatermark(processor.Watermark(time.Unix(epoch-120, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(0) }))

	keys := p.getAllKeysFromBucket()
	assert.Equal(t, []string{"1651161600", "1651161660", "1651161720"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), head.String())

	p.StopPublisher()

	// run this test only if we are not sharing the bucket
	if !publishEntity.IsSharedBucket() {
		_, err = js.KeyValue(publishEntity.GetBucketName())
		assert.Equal(t, nats.ErrBucketNotFound, err)
	}

	_, err = p.publishStore.GetValue(ctx, publishEntity.GetID())
	assert.Equal(t, nats.ErrKeyNotFound, err)
}
