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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/aligned/memory"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
)

const (
	testPipelineName    = "testPipeline"
	testProcessorEntity = "publisherTestPod"
	publisherKeyspace   = testPipelineName + "_" + testProcessorEntity + "_%s"
)

type pbqReader struct {
}

func (p *pbqReader) ReadCh() <-chan *window.TimedWindowRequest {
	return nil
}

func (p *pbqReader) GC() error {
	return nil
}

type forwardTest struct {
	count   int
	buffers []string
}

func (f *forwardTest) WhereTo(_ []string, _ []string) ([]forwarder.VertexBuffer, error) {
	var steps []forwarder.VertexBuffer
	for _, buffer := range f.buffers {
		steps = append(steps, forwarder.VertexBuffer{
			ToVertexName:         buffer,
			ToVertexPartitionIdx: int32(f.count % 2),
		})
	}
	f.count++
	return steps, nil
}

var keyedVertex = &dfv1.VertexInstance{
	Vertex: &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "test-pipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
			UDF:  &dfv1.UDF{GroupBy: &dfv1.GroupBy{Keyed: true}},
		},
	}},
	Hostname: "test-host",
	Replica:  0,
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
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer1-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(dfv1.DiscardLatest)),
				simplebuffer.NewInMemoryBuffer("buffer1-2", 10, 1, simplebuffer.WithBufferFullWritingStrategy(dfv1.DiscardLatest))},
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name: "test-retry-until-success",
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer2-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(dfv1.RetryUntilSuccess)),
				simplebuffer.NewInMemoryBuffer("buffer2-2", 10, 0, simplebuffer.WithBufferFullWritingStrategy(dfv1.RetryUntilSuccess))},
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

func TestPnFHandleAlignedWindowResponses(t *testing.T) {
	var (
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		responseCh  = make(chan *window.TimedWindowResponse)
		errCh       = make(chan error)
		wg          = &sync.WaitGroup{}
	)
	defer cancel()
	id := &partition.ID{
		Start: time.UnixMilli(60000),
		End:   time.UnixMilli(120000),
		Slot:  "slot-0",
	}

	test1Buffer11 := simplebuffer.NewInMemoryBuffer("buffer1-1", 5, 0)
	test1Buffer12 := simplebuffer.NewInMemoryBuffer("buffer1-2", 5, 1)

	test1Buffer21 := simplebuffer.NewInMemoryBuffer("buffer2-1", 5, 0)
	test1Buffer22 := simplebuffer.NewInMemoryBuffer("buffer2-2", 5, 1)

	toBuffersMap := map[string][]isb.BufferWriter{
		"buffer1": {test1Buffer11, test1Buffer12},
		"buffer2": {test1Buffer21, test1Buffer22},
	}

	buffers := make([]string, 0)
	for k := range toBuffersMap {
		buffers = append(buffers, k)
	}

	whereto := &forwardTest{
		buffers: buffers,
	}

	responses := generateAlignedWindowResponses(10, id)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, response := range responses {
			responseCh <- response
		}
		close(responseCh)
	}()

	windower := fixed.NewWindower(60*time.Second, keyedVertex)
	windower.InsertWindow(window.NewAlignedTimedWindow(id.Start, id.End, id.Slot))

	wmPublishers, _ := buildPublisherMapAndOTStore(toBuffersMap)
	latestWriteOffsets := make(map[string][][]isb.Offset)
	for toVertexName, toVertexBuffer := range toBuffersMap {
		latestWriteOffsets[toVertexName] = make([][]isb.Offset, len(toVertexBuffer))
	}

	idleManager, _ := wmb.NewIdleManager(1, len(toBuffersMap))

	pf := &processAndForward{
		partitionId:        id,
		whereToDecider:     whereto,
		windower:           windower,
		toBuffers:          toBuffersMap,
		wmPublishers:       wmPublishers,
		idleManager:        idleManager,
		pbqReader:          &pbqReader{},
		done:               make(chan struct{}),
		latestWriteOffsets: latestWriteOffsets,
		log:                logging.FromContext(ctx),
	}

	pf.forwardAlignedWindowResponses(ctx, responseCh, errCh)

	wg.Wait()

	for _, writeOffsets := range latestWriteOffsets {
		for _, offsets := range writeOffsets {
			println(offsets[0].Sequence())
		}
	}
	assert.Equal(t, true, test1Buffer11.IsFull())
	assert.Equal(t, true, test1Buffer12.IsFull())
	assert.Equal(t, true, test1Buffer21.IsFull())
	assert.Equal(t, true, test1Buffer22.IsFull())
}

func generateAlignedWindowResponses(count int, id *partition.ID) []*window.TimedWindowResponse {
	var windowResponses []*window.TimedWindowResponse

	for i := 0; i <= count; i++ {
		if i == count {
			windowResponses = append(windowResponses, &window.TimedWindowResponse{
				Window: window.NewAlignedTimedWindow(id.Start, id.End, id.Slot),
				EOF:    true,
			})
			continue
		}
		windowResponses = append(windowResponses, &window.TimedWindowResponse{
			WriteMessage: &isb.WriteMessage{
				Message: isb.Message{
					Header: isb.Header{
						MessageInfo: isb.MessageInfo{
							EventTime: id.End.Add(-1 * time.Millisecond),
						},
						ID:   fmt.Sprintf("%d-testVertex-0-0", i), // TODO: hard coded ID suffix ATM, make configurable if needed
						Keys: []string{},
					},
					Body: isb.Body{Payload: []byte("test")},
				},
			},
			Window: window.NewAlignedTimedWindow(id.Start, id.End, id.Slot),
			EOF:    false,
		})
	}
	return windowResponses
}

func createProcessAndForwardAndOTStore(ctx context.Context, key string, pbqManager *pbq.Manager, toBuffers map[string][]isb.BufferWriter) (processAndForward, map[string]kvs.KVStorer) {

	testPartition := &partition.ID{
		Start: time.UnixMilli(60000),
		End:   time.UnixMilli(120000),
		Slot:  key,
	}

	// create a pbq for a partition
	pw, otStore := buildPublisherMapAndOTStore(toBuffers)
	var simplePbq pbq.Reader
	simplePbq, _ = pbqManager.CreateNewPBQ(ctx, *testPartition)

	buffers := make([]string, 0)
	for k := range toBuffers {
		buffers = append(buffers, k)
	}
	whereto := &forwardTest{
		buffers: buffers,
	}

	idleManager, _ := wmb.NewIdleManager(1, len(toBuffers))

	pf := processAndForward{
		partitionId:    testPartition,
		UDF:            nil,
		pbqReader:      simplePbq,
		log:            logging.FromContext(ctx),
		toBuffers:      toBuffers,
		whereToDecider: whereto,
		wmPublishers:   pw,
		idleManager:    idleManager,
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
