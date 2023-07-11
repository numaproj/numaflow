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
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"go.uber.org/zap/zaptest"
)

func test_EdgeFetcherSet_ProcessOffsetGetWatermark(t *testing.T) {
	var ctx = context.Background()

	// test fetching from 2 edges

	numIncomingVertices := 2
	partitionCount := int32(3)
	processorManagers := make([]*processor.ProcessorManager, 0)
	for i := 0; i < numIncomingVertices; i++ {
		processorManagers = append(processorManagers, createProcessorManager(ctx, partitionCount))
	}

	testPodTimelines := [][][]wmb.WMB{
		// first vertex
		{
			// first Pod in first vertex
			{
				{Watermark: 11, Offset: 9, Partition: 0},
				{Watermark: 12, Offset: 20, Partition: 1},
				{Watermark: 13, Offset: 21, Partition: 2},
				{Watermark: 14, Offset: 22, Partition: 0},
				{Watermark: 17, Offset: 28, Partition: 1},
				{Watermark: 25, Offset: 30, Partition: 2},
				{Watermark: 26, Offset: 31, Partition: 0},
				{Watermark: 27, Offset: 32, Partition: 1},
			},
			// second Pod in first vertex
			{
				{Watermark: 8, Offset: 13, Partition: 0},
				{Watermark: 9, Offset: 16, Partition: 1},
				{Watermark: 10, Offset: 18, Partition: 2},
				{Watermark: 17, Offset: 26, Partition: 0},
				{Watermark: 27, Offset: 29, Partition: 1},
				{Watermark: 28, Offset: 33, Partition: 2},
				{Watermark: 29, Offset: 34, Partition: 0},
			},
		},
		// second vertex
		{
			// only Pod in second vertex
			{
				{Watermark: 10, Offset: 14, Partition: 0},
				{Watermark: 12, Offset: 17, Partition: 1},
				{Watermark: 14, Offset: 19, Partition: 2},
				{Watermark: 17, Offset: 24, Partition: 0},
				{Watermark: 25, Offset: 35, Partition: 1},
				{Watermark: 26, Offset: 36, Partition: 2},
				{Watermark: 27, Offset: 37, Partition: 0},
			},
		},
	}

	testPodsByVertex := make([][]*processor.ProcessorToFetch, numIncomingVertices)
	for vertex := 0; vertex < numIncomingVertices; vertex++ {
		numPods := len(testPodTimelines[vertex])
		testPodsByVertex[vertex] = make([]*processor.ProcessorToFetch, numPods)
		for pod := 0; pod < numPods; pod++ {
			name := fmt.Sprintf("test-pod-%d-%d", vertex, pod)
			testPodsByVertex[vertex][pod] = processor.NewProcessorToFetch(ctx, processor.NewProcessorEntity(name), "test-bucket", 5, partitionCount)
			for _, watermark := range testPodTimelines[vertex][pod] {
				testPodsByVertex[vertex][pod].GetOffsetTimelines()[watermark.Partition].Put(watermark)
			}
			processorManagers[vertex].AddProcessor(name, testPodsByVertex[vertex][pod])
		}

	}

	tests := []struct {
		name            string
		offset          int64
		partitionIdx    int32
		want            int64
		lastProcessedWm [][]int64 // last processed watermark for each pod on each vertex
	}{
		{
			name:         "offset_9",
			offset:       9,
			want:         -1,
			partitionIdx: 0,
			lastProcessedWm: [][]int64{
				{-1, -1},
				{-1},
			},
		},
	}

	location, _ := time.LoadLocation("UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create EdgeFetcherSet with 2 EdgeFetchers
			efs := &edgeFetcherSet{
				edgeFetchers: map[string]Fetcher{},
				log:          zaptest.NewLogger(t).Sugar(),
			}
			for vertex := 0; vertex < numIncomingVertices; vertex++ {
				vertexName := fmt.Sprintf("vertex-%d", vertex)
				efs.edgeFetchers[vertexName] = &EdgeFetcher{
					ctx:              ctx,
					processorManager: processorManagers[vertex],
					log:              zaptest.NewLogger(t).Sugar(),
					lastProcessedWm:  tt.lastProcessedWm[vertex],
				}
			}
			if got := efs.ProcessOffsetGetWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.offset, 10) }), tt.partitionIdx); time.Time(got).In(location) != time.UnixMilli(tt.want).In(location) {
				t.Errorf("GetWatermark() = %v, want %v", got, wmb.Watermark(time.UnixMilli(tt.want)))
			}

			//todo: add test of GetHeadWatermark like edge_fetcher_test.go??
		})
	}

}

func createProcessorManager(ctx context.Context, partitionCount int32) *processor.ProcessorManager {
	hbWatcher := noop.NewKVOpWatch()
	otWatcher := noop.NewKVOpWatch()
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	return processor.NewProcessorManager(ctx, storeWatcher, "test-bucket", partitionCount)
}
