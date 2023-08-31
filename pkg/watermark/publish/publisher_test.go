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

package publish

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

func createAndLaterDeleteBucket(js nats.JetStreamContext, kvConfig *nats.KeyValueConfig) (func(), error) {
	kv, err := js.CreateKeyValue(kvConfig)
	if err != nil {
		return nil, err
	}
	return func() {
		// ignore error as this will be executed only via defer
		_ = js.DeleteKeyValue(kv.Bucket())
	}, nil
}

func TestPublisherWithSharedOTBucket(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	var ctx = context.Background()

	defaultJetStreamClient := natstest.JetStreamClient(t, s)
	defer defaultJetStreamClient.Close()
	js, err := defaultJetStreamClient.JetStreamContext()
	assert.NoError(t, err)

	var keyspace = "publisherTest"

	deleteFn, err := createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: keyspace + "_PROCESSORS"})
	assert.NoError(t, err)
	defer deleteFn()

	deleteFn, err = createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: keyspace + "_OT"})
	assert.NoError(t, err)
	defer deleteFn()

	publishEntity := entity.NewProcessorEntity("publisherTestPod1")

	wmstore, err := store.BuildJetStreamWatermarkStore(ctx, keyspace, defaultJetStreamClient)
	assert.NoError(t, err)

	p := NewPublish(ctx, publishEntity, wmstore, 1, WithAutoRefreshHeartbeatDisabled(), WithPodHeartbeatRate(1)).(*publish)

	var epoch int64 = 1651161600000
	var location, _ = time.LoadLocation("UTC")
	for i := 0; i < 3; i++ {
		p.PublishWatermark(wmb.Watermark(time.UnixMilli(epoch).In(location)), isb.SimpleStringOffset(func() string { return strconv.Itoa(i) }), 0)
		epoch += 60000
		time.Sleep(time.Millisecond)
	}
	// publish a stale watermark (offset doesn't matter)
	p.PublishWatermark(wmb.Watermark(time.UnixMilli(epoch-120000).In(location)), isb.SimpleStringOffset(func() string { return strconv.Itoa(0) }), 0)

	keys, err := p.otStore.GetAllKeys(p.ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"publisherTestPod1"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, wmb.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, wmb.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), head.String())

	// simulate the next read batch where the watermark is now $epoch
	// set offset to -1 to simulate reduce vertex publish idle watermark
	p.PublishIdleWatermark(wmb.Watermark(time.UnixMilli(epoch).In(location)), isb.SimpleStringOffset(func() string { return strconv.Itoa(-1) }), 0)
	keys, err = p.otStore.GetAllKeys(p.ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"publisherTestPod1"}, keys)
	otValue, err := p.otStore.GetValue(p.ctx, keys[0])
	assert.NoError(t, err)
	otDecode, err := wmb.DecodeToWMB(otValue)
	assert.NoError(t, err)
	assert.True(t, otDecode.Idle)
	assert.Equal(t, otDecode.Watermark, epoch)
	assert.Equal(t, otDecode.Offset, int64(-1))
	assert.Equal(t, otDecode.Partition, int32(0))

	_ = p.Close()

	_, err = p.heartbeatStore.GetValue(ctx, publishEntity.GetName())
	assert.Equal(t, nats.ErrKeyNotFound, err)
}
