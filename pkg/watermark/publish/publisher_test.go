//go:build isb_jetstream

package publish

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/stretchr/testify/assert"
)

func TestPublisherWithSeparateOTBuckets(t *testing.T) {
	var ctx = context.Background()

	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	assert.Nil(t, err)

	// Create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	assert.Nil(t, err)

	var keyspace = "publisherTest"

	publishHeartbeatBucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_PROCESSORS",
		Description:  fmt.Sprintf("[%s] heartbeat bucket", keyspace),
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      0,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_PROCESSORS") }()
	publishEntity := processor.NewProcessorEntity("publisherTestPod1", keyspace)

	p := NewPublish(ctx, publishEntity, keyspace, js, publishHeartbeatBucket,
		WithAutoRefreshHeartbeat(true),
		WithPodHeartbeatRate(1),
		WithBucketConfigs(&nats.KeyValueConfig{
			Bucket:       publishEntity.GetBucketName(),
			Description:  fmt.Sprintf("[%s][%s] offset timeline bucket", keyspace, publishEntity.GetBucketName()),
			MaxValueSize: 0,
			History:      1,
			TTL:          0,
			MaxBytes:     0,
			Storage:      nats.MemoryStorage,
			Replicas:     0,
			Placement:    nil,
		}))
	defer func() { _ = js.DeleteKeyValue(publishEntity.GetBucketName()) }()
	var epoch int64 = 1651161600
	var location, _ = time.LoadLocation("UTC")
	for i := 0; i < 3; i++ {
		p.PublishWatermark(processor.Watermark(time.Unix(epoch, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(i) }))
		epoch += 60
		time.Sleep(time.Millisecond)
	}
	// publish a stale watermark (offset doesn't matter)
	p.PublishWatermark(processor.Watermark(time.Unix(epoch-120, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(0) }))

	keys := p.getAllKeysFromBucket(publishEntity.GetBucketName())
	assert.Equal(t, []string{"publisherTestPod1_1651161600", "publisherTestPod1_1651161660", "publisherTestPod1_1651161720"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), head.String())

	p.Cleanup()

	_, err = js.KeyValue(publishEntity.GetBucketName())
	assert.Equal(t, nats.ErrBucketNotFound, err)

	_, err = p.heartbeatBucket.Get(publishEntity.GetID())
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

	publishHeartbeatBucket, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace + "_PROCESSORS",
		Description:  fmt.Sprintf("[%s] heartbeat bucket", keyspace),
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      0,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace + "_PROCESSORS") }()
	publishEntity := processor.NewProcessorEntity("publisherTestPod1", keyspace, processor.WithSeparateOTBuckets(true))

	p := NewPublish(ctx, publishEntity, keyspace, js, publishHeartbeatBucket,
		WithAutoRefreshHeartbeat(true),
		WithPodHeartbeatRate(1),
		WithBucketConfigs(&nats.KeyValueConfig{
			Bucket:       publishEntity.GetBucketName(),
			Description:  fmt.Sprintf("[%s][%s] offset timeline bucket", keyspace, publishEntity.GetBucketName()),
			MaxValueSize: 0,
			History:      1,
			TTL:          0,
			MaxBytes:     0,
			Storage:      nats.MemoryStorage,
			Replicas:     0,
			Placement:    nil,
		}))
	defer func() { _ = js.DeleteKeyValue(publishEntity.GetBucketName()) }()
	var epoch int64 = 1651161600
	var location, _ = time.LoadLocation("UTC")
	for i := 0; i < 3; i++ {
		p.PublishWatermark(processor.Watermark(time.Unix(epoch, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(i) }))
		epoch += 60
		time.Sleep(time.Millisecond)
	}
	// publish a stale watermark (offset doesn't matter)
	p.PublishWatermark(processor.Watermark(time.Unix(epoch-120, 0).In(location)), isb.SimpleOffset(func() string { return strconv.Itoa(0) }))

	keys := p.getAllKeysFromBucket(publishEntity.GetBucketName())
	assert.Equal(t, []string{"1651161600", "1651161660", "1651161720"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.Unix(epoch-60, 0).In(location)).String(), head.String())

	p.Cleanup()

	_, err = js.KeyValue(publishEntity.GetBucketName())
	assert.Equal(t, nats.ErrBucketNotFound, err)

	_, err = p.heartbeatBucket.Get(publishEntity.GetID())
	assert.Equal(t, nats.ErrKeyNotFound, err)

}
