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
package pnf

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/memory"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
)

const (
	testPipelineName    = "testPipeline"
	testProcessorEntity = "publisherTestPod"
	publisherKeyspace   = testPipelineName + "_" + testProcessorEntity + "_%s"
)

type forwardTest struct {
	count   int
	buffers []string
}

func (f *forwardTest) WhereTo(keys []string, _ []string) ([]forwarder.VertexBuffer, error) {
	if strings.Compare(keys[len(keys)-1], "test-forward-one") == 0 {
		return []forwarder.VertexBuffer{{
			ToVertexName:         "buffer1",
			ToVertexPartitionIdx: int32(f.count % 2),
		}}, nil
	} else if strings.Compare(keys[len(keys)-1], "test-forward-all") == 0 {
		var steps []forwarder.VertexBuffer
		for _, buffer := range f.buffers {
			steps = append(steps, forwarder.VertexBuffer{
				ToVertexName:         buffer,
				ToVertexPartitionIdx: int32(f.count % 2),
			})
		}
		return steps, nil
	}
	f.count++
	return []forwarder.VertexBuffer{}, nil
}

func (f *forwardTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.WriteMessage, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

type PayloadForTest struct {
	Key   string
	Value int64
}

// TestWriteToBuffer tests two BufferFullWritingStrategies: 1. discarding the latest message and 2. retrying writing until context is cancelled.
func TestWriteToBuffer(t *testing.T) {
	tests := []struct {
		name       string
		buffers    []isb.BufferWriter
		throwError bool
	}{
		{
			name: "test-discard-latest",
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer1-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.DiscardLatest)),
				simplebuffer.NewInMemoryBuffer("buffer1-2", 10, 1, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.DiscardLatest))},
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name: "test-retry-until-success",
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer2-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.RetryUntilSuccess)),
				simplebuffer.NewInMemoryBuffer("buffer2-2", 10, 0, simplebuffer.WithBufferFullWritingStrategy(v1alpha1.RetryUntilSuccess))},
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
	}
	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			testStartTime := time.Unix(1636470000, 0).UTC()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var pbqManager *pbq.Manager
			pbqManager, _ = pbq.NewManager(ctx, "reduce", "test-pipeline", 0, memory.NewMemoryStores(), window.Aligned)
			toBuffer := map[string][]isb.BufferWriter{
				"buffer": value.buffers,
			}
			pf, _ := createProcessAndForwardAndOTStore(ctx, value.name, pbqManager, toBuffer)
			windowResponse := testutils.BuildTestWriteMessages(int64(15), testStartTime)
			pf.writeToBuffer(ctx, "buffer", 0, windowResponse)
		})
	}
}

func createProcessAndForwardAndOTStore(ctx context.Context, key string, pbqManager *pbq.Manager, toBuffers map[string][]isb.BufferWriter) (processAndForward, map[string]kvs.KVStorer) {

	testPartition := partition.ID{
		Start: time.UnixMilli(60000),
		End:   time.UnixMilli(120000),
		Slot:  key,
	}

	// create a pbq for a partition
	pw, otStore := buildPublisherMapAndOTStore(toBuffers)
	var simplePbq pbq.Reader
	simplePbq, _ = pbqManager.CreateNewPBQ(ctx, testPartition)

	buffers := make([]string, 0)
	for k := range toBuffers {
		buffers = append(buffers, k)
	}
	whereto := &forwardTest{
		buffers: buffers,
	}

	pf := processAndForward{
		PartitionID:    testPartition,
		UDF:            nil,
		pbqReader:      simplePbq,
		log:            logging.FromContext(ctx),
		toBuffers:      toBuffers,
		whereToDecider: whereto,
		wmPublishers:   pw,
		idleManager:    wmb.NewIdleManager(len(toBuffers)),
	}

	return pf, otStore
}

// buildPublisherMapAndOTStore builds OTStore and publisher for each toBuffer
func buildPublisherMapAndOTStore(toBuffers map[string][]isb.BufferWriter) (map[string]publish.Publisher, map[string]kvs.KVStorer) {
	var ctx = context.Background()
	processorEntity := entity.NewProcessorEntity("publisherTestPod")
	publishers := make(map[string]publish.Publisher)
	otStores := make(map[string]kvs.KVStorer)
	for key, partitionedBuffers := range toBuffers {
		store, _ := wmstore.BuildInmemWatermarkStore(ctx, publisherKeyspace)
		otStores[key] = store.OffsetTimelineStore()
		p := publish.NewPublish(ctx, processorEntity, store, int32(len(partitionedBuffers)), publish.WithAutoRefreshHeartbeatDisabled(), publish.WithPodHeartbeatRate(1))
		publishers[key] = p
	}
	return publishers, otStores
}
