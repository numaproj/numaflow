//go:build isb_jetstream

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
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
)

func createAndLaterDeleteBucket(js *jsclient.JetStreamContext, kvConfig *nats.KeyValueConfig) (func(), error) {
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

func TestPublisherWithSharedOTBucket(t *testing.T) {
	var ctx = context.Background()

	defaultJetStreamClient := jsclient.NewDefaultJetStreamClient(nats.DefaultURL)
	conn, err := defaultJetStreamClient.Connect(ctx)
	assert.NoError(t, err)
	js, err := conn.JetStream()
	assert.NoError(t, err)

	var keyspace = "publisherTest"

	deleteFn, err := createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: keyspace + "_PROCESSORS"})
	defer deleteFn()

	deleteFn, err = createAndLaterDeleteBucket(js, &nats.KeyValueConfig{Bucket: keyspace + "_OT"})
	defer deleteFn()

	publishEntity := processor.NewProcessorEntity("publisherTestPod1")

	heartbeatKV, err := jetstream.NewKVJetStreamKVStore(ctx, "testPublisher", keyspace+"_PROCESSORS", defaultJetStreamClient)
	assert.NoError(t, err)
	otKV, err := jetstream.NewKVJetStreamKVStore(ctx, "testPublisher", keyspace+"_OT", defaultJetStreamClient)
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

	keys, err := p.otStore.GetAllKeys(p.ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"publisherTestPod1"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), head.String())

	p.StopPublisher()

	_, err = p.heartbeatStore.GetValue(ctx, publishEntity.GetID())
	assert.Equal(t, nats.ErrConnectionClosed, err)
}
