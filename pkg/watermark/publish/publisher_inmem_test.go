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

	keys, err := p.otStore.GetAllKeys(p.ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"publisherTestPod1"}, keys)

	wm := p.loadLatestFromStore()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), wm.String())

	head := p.GetLatestWatermark()
	assert.Equal(t, processor.Watermark(time.UnixMilli(epoch-60000).In(location)).String(), head.String())

	p.StopPublisher()

	_, err = p.heartbeatStore.GetValue(ctx, publishEntity.GetID())
	assert.Equal(t, fmt.Errorf("key publisherTestPod1 not found"), err)

}
