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

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

func Test_EdgeFetcherSet_ComputeWatermark(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// test fetching from 2 edges

	numIncomingVertices := 2
	partitionCount := int32(3)
	processorManagers := make([]*processorManager, 0)
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

	testPodsByVertex := make([][]*ProcessorToFetch, numIncomingVertices)
	for vertex := 0; vertex < numIncomingVertices; vertex++ {
		numPods := len(testPodTimelines[vertex])
		testPodsByVertex[vertex] = make([]*ProcessorToFetch, numPods)
		for pod := 0; pod < numPods; pod++ {
			name := fmt.Sprintf("test-pod-%d-%d", vertex, pod)
			testPodsByVertex[vertex][pod] = NewProcessorToFetch(ctx, entity.NewProcessorEntity(name), 5, partitionCount)
			for _, watermark := range testPodTimelines[vertex][pod] {
				testPodsByVertex[vertex][pod].GetOffsetTimelines()[watermark.Partition].Put(watermark)
			}
			processorManagers[vertex].addProcessor(name, testPodsByVertex[vertex][pod])
		}

	}

	tests := []struct {
		name            string
		offset          int64
		partitionIdx    int32
		want            int64
		lastProcessedWm [][]int64 // last processed watermark for each partition for each edge
	}{

		{
			// test case where we end up using one of the lastProcessedWms since it's smallest
			// for first EdgeFetcher:
			// // offset 23 on partition 0 will produce WM 8
			// // if lastProcessedWm on other partitions is 7, we take 7 since 7<8
			// for second EdgeFetcher:
			// // offset 23 on partition 0 will produce WM 10
			// // if lastProcessedWm on other partitions is 9, we take 9 since 9<10
			// then we compare EdgeFetchers 1 and 2 and get 7 since 7<9
			name:         "useLastProcessedWm",
			offset:       23,
			want:         7,
			partitionIdx: 0,
			lastProcessedWm: [][]int64{
				{7, 7, 7},
				{9, 9, 9},
			},
		},
		{
			// test case where we end up using the newly calculated watermark for one of the EdgeFetchers since it's smallest
			// for first EdgeFetcher:
			// // offset 23 on partition 0 will produce WM 8
			// // if lastProcessedWm on other partitions is 9, we take 8 since 8<9
			// for second EdgeFetcher:
			// // offset 23 on partition 0 will produce WM 10
			// // if lastProcessedWm on other partitions is 9, we take 9 since 9<10
			// then we compare EdgeFetchers 1 and 2 and get 8 since 8<9
			name:         "useLastProcessedWm",
			offset:       23,
			want:         8,
			partitionIdx: 0,
			lastProcessedWm: [][]int64{
				{6, 9, 9},
				{9, 9, 9},
			},
		},
		{
			// test case in which other partitions haven't been processed yet
			name:         "unprocessedPartitions",
			offset:       15,
			want:         -1,
			partitionIdx: 1,
			lastProcessedWm: [][]int64{
				{-1, -1, -1},
				{-1, -1, -1},
			},
		},
	}

	location, _ := time.LoadLocation("UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create EdgeFetcherSet with 2 EdgeFetchers
			efs := &edgeFetcherSet{
				edgeFetchers: map[string]*edgeFetcher{},
				log:          zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel)).Sugar(),
			}

			for vertex := 0; vertex < numIncomingVertices; vertex++ {
				vertexName := fmt.Sprintf("vertex-%d", vertex)
				efs.edgeFetchers[vertexName] = &edgeFetcher{
					processorManager: processorManagers[vertex],
					log:              zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel)).Sugar(),
					lastProcessedWm:  tt.lastProcessedWm[vertex],
				}
			}
			if got := efs.ComputeWatermark(isb.SimpleStringOffset(func() string { return strconv.FormatInt(tt.offset, 10) }), tt.partitionIdx); time.Time(got).In(location) != time.UnixMilli(tt.want).In(location) {
				t.Errorf("ComputeWatermark() = %v, want %v", got, wmb.Watermark(time.UnixMilli(tt.want)))
			}

		})
	}

}

func createProcessorManager(ctx context.Context, partitionCount int32) *processorManager {
	wmStore, _ := store.BuildNoOpWatermarkStore()
	return newProcessorManager(ctx, wmStore, partitionCount)
}

func edge1Idle(ctx context.Context, processorManager *processorManager) {
	var (
		testPod0     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, 2)
		testPod1     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, 2)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    28,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    27,
				Watermark: 16,
				Partition: 1,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    25,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    24,
				Watermark: 16,
				Partition: 1,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager.addProcessor("testPod0", testPod0)
	processorManager.addProcessor("testPod1", testPod1)
}

func edge1NonIdle(ctx context.Context, processorManager *processorManager) {
	var (
		testPod0     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, 2)
		testPod1     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, 2)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    27,
				Watermark: 16,
				Partition: 1,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    25,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    24,
				Watermark: 16,
				Partition: 1,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager.addProcessor("testPod0", testPod0)
	processorManager.addProcessor("testPod1", testPod1)
}

func edge2Idle(ctx context.Context, processorManager *processorManager) {
	var (
		testPod0     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, 2)
		testPod1     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, 2)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 18,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    16,
				Watermark: 15,
				Partition: 1,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      true,
				Offset:    26,
				Watermark: 19,
				Partition: 0,
			},
			{
				Idle:      true,
				Offset:    24,
				Watermark: 22,
				Partition: 1,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager.addProcessor("testPod0", testPod0)
	processorManager.addProcessor("testPod1", testPod1)
}

func edge2NonIdle(ctx context.Context, processorManager *processorManager) {
	var (
		testPod0     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod1"), 5, 2)
		testPod1     = NewProcessorToFetch(ctx, entity.NewProcessorEntity("testPod2"), 5, 2)
		pod0Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    28,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    27,
				Watermark: 16,
				Partition: 1,
			},
		}
		pod1Timeline = []wmb.WMB{
			{
				Idle:      false,
				Offset:    25,
				Watermark: 17,
				Partition: 0,
			},
			{
				Idle:      false,
				Offset:    24,
				Watermark: 16,
				Partition: 1,
			},
		}
	)

	for _, watermark := range pod0Timeline {
		testPod0.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	for _, watermark := range pod1Timeline {
		testPod1.GetOffsetTimelines()[watermark.Partition].Put(watermark)
	}
	processorManager.addProcessor("testPod0", testPod0)
	processorManager.addProcessor("testPod1", testPod1)
}

func Test_EdgeFetcherSet_GetHeadWMB(t *testing.T) {

	// cases to test:
	// (should test 1 Edge Fetcher as well as 2)
	// 1. one of them has all publishers Idle and 1 doesn't: should return WMB{}
	// 2. all publishers Idle: should not return WMB{} and should return most conservative Watermark
	// 3. all publishers Idle but somehow the GetWatermark() of one of the EdgeFetchers is higher than the returned value

	var (
		ctx, cancel = context.WithCancel(context.Background())
		wmStore, _  = store.BuildNoOpWatermarkStore()

		edge1ProcessorManagerIdle    = newProcessorManager(ctx, wmStore, 2)
		edge1ProcessorManagerNonIdle = newProcessorManager(ctx, wmStore, 2)

		edge2ProcessorManagerIdle    = newProcessorManager(ctx, wmStore, 2)
		edge2ProcessorManagerNonIdle = newProcessorManager(ctx, wmStore, 2)
	)

	defer cancel()

	edge1Idle(ctx, edge1ProcessorManagerIdle)
	edge1NonIdle(ctx, edge1ProcessorManagerNonIdle)

	edge2Idle(ctx, edge2ProcessorManagerIdle)
	edge2NonIdle(ctx, edge2ProcessorManagerNonIdle)

	tests := []struct {
		name         string
		edgeFetchers map[string]*edgeFetcher
		expectedWMB  wmb.WMB
	}{
		{
			"1_edge_non_idle",
			map[string]*edgeFetcher{
				"non_idle": {
					processorManager: edge1ProcessorManagerNonIdle,
					lastProcessedWm:  []int64{16, 18},
					log:              zaptest.NewLogger(t).Sugar(),
				},
			},
			wmb.WMB{},
		},
		{
			"1_edge_idle",
			map[string]*edgeFetcher{
				"idle": {
					processorManager: edge1ProcessorManagerIdle,
					lastProcessedWm:  []int64{16, 18},
					log:              zaptest.NewLogger(t).Sugar(),
				},
			},
			wmb.WMB{
				Idle:      true,
				Offset:    24,
				Watermark: 16,
				Partition: 1,
			},
		},
		{
			"2_edges_non_idle",
			map[string]*edgeFetcher{
				"edge1_non_idle": {
					processorManager: edge1ProcessorManagerNonIdle,
					lastProcessedWm:  []int64{16, 18},
					log:              zaptest.NewLogger(t).Sugar(),
				},
				"edge2_non_idle": {
					processorManager: edge2ProcessorManagerNonIdle,
					lastProcessedWm:  []int64{17, 17},
					log:              zaptest.NewLogger(t).Sugar(),
				},
			},
			wmb.WMB{},
		},
		{
			"2_edges_mixed",
			map[string]*edgeFetcher{
				"edge1_idle": {
					processorManager: edge1ProcessorManagerIdle,
					lastProcessedWm:  []int64{16, 18},
					log:              zaptest.NewLogger(t).Sugar(),
				},
				"edge2_non_idle": {
					processorManager: edge2ProcessorManagerNonIdle,
					lastProcessedWm:  []int64{17, 17},
					log:              zaptest.NewLogger(t).Sugar(),
				},
			},
			wmb.WMB{},
		},
		{
			"2_edges_idle",
			map[string]*edgeFetcher{
				"edge1_idle": {
					processorManager: edge1ProcessorManagerIdle,
					lastProcessedWm:  []int64{19, 18},
					log:              zaptest.NewLogger(t).Sugar(),
				},
				"edge2_idle": {
					processorManager: edge2ProcessorManagerIdle,
					lastProcessedWm:  []int64{15, 18},
					log:              zaptest.NewLogger(t).Sugar(),
				},
			},
			wmb.WMB{
				Idle:      true,
				Offset:    16,
				Watermark: 15,
				Partition: 1,
			},
		},
		{
			"2_edges_idle_larger_than_last_processed_watermark",
			map[string]*edgeFetcher{
				"edge1_idle": {
					processorManager: edge1ProcessorManagerIdle,
					lastProcessedWm:  []int64{13, 15},
					log:              zaptest.NewLogger(t).Sugar(),
				},
				"edge2_idle": {
					processorManager: edge2ProcessorManagerIdle,
					lastProcessedWm:  []int64{14, 12},
					log:              zaptest.NewLogger(t).Sugar(),
				},
			},
			wmb.WMB{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			efs := &edgeFetcherSet{
				edgeFetchers: tt.edgeFetchers,
				log:          zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel)).Sugar(),
			}
			headWMB := efs.ComputeHeadIdleWMB(1)
			assert.Equal(t, tt.expectedWMB, headWMB)
		})
	}
}
