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

// Package service is built for querying metadata and to expose it over daemon service.
package service

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
)

// TODO - Write Unit Tests for this file

// GetVertexWatermarkFetchers returns a map of the watermark fetchers, where key is the vertex name,
// value is a list of fetchers to all the outgoing buffers of that vertex.
func GetVertexWatermarkFetchers(ctx context.Context, pipeline *v1alpha1.Pipeline, isbSvcClient isbsvc.ISBService) (map[string][]fetch.Fetcher, error) {
	var wmFetchers = make(map[string][]fetch.Fetcher)
	if pipeline.Spec.Watermark.Disabled {
		return wmFetchers, nil
	}

	for _, vertex := range pipeline.Spec.Vertices {
		// key for fetcher map ~ vertexName/replicas
		replicas := int64(1)
		if vertex.Sink != nil {
			toBufferName := v1alpha1.GenerateSinkBufferName(pipeline.Namespace, pipeline.Name, vertex.Name)
			wmFetcher, err := isbSvcClient.CreateWatermarkFetcher(ctx, toBufferName)
			if err != nil {
				return nil, fmt.Errorf("failed to create watermark fetcher, %w", err)
			}
			// for now only reduce has parallelism so not updating replicas for sink for now as done below
			fetchersKey := vertex.Name + "/" + strconv.FormatInt(replicas, 10)
			wmFetchers[fetchersKey] = []fetch.Fetcher{wmFetcher}
		} else {
			// If the vertex is not a sink, to fetch the watermark, we consult all out edges and grab the latest watermark among them.
			var wmFetcherList []fetch.Fetcher
			for _, edge := range pipeline.GetToEdges(vertex.Name) {
				toBufferNames := v1alpha1.GenerateEdgeBufferNames(pipeline.Namespace, pipeline.Name, edge)
				for _, toBufferName := range toBufferNames {
					fetchWatermark, err := isbSvcClient.CreateWatermarkFetcher(ctx, toBufferName)
					if err != nil {
						return nil, fmt.Errorf("failed to create watermark fetcher  %w", err)
					}
					wmFetcherList = append(wmFetcherList, fetchWatermark)
				}
			}
			// for now only reduce has parallelism might have to modify later
			// checking parallelism for a vertex to identify reduce vertex
			// replicas will have parallelism for reduce vertex else will be nil
			// parallelism indicates replica count ~ multiple pods for a vertex here
			obj := pipeline.GetFromEdges(vertex.Name)
			if len(obj) > 0 && obj[0].Parallelism != nil {
				replicas = int64(*obj[0].Parallelism)
			}
			fetchersKey := vertex.Name + "/" + strconv.FormatInt(replicas, 10)
			wmFetchers[fetchersKey] = wmFetcherList
		}
	}
	return wmFetchers, nil
}

// GetVertexWatermark is used to return the head watermark for a given vertex.
func (ps *pipelineMetadataQuery) GetVertexWatermark(ctx context.Context, request *daemon.GetVertexWatermarkRequest) (*daemon.GetVertexWatermarkResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.GetVertexWatermarkResponse)
	vertexName := request.GetVertex()
	isWatermarkEnabled := !ps.pipeline.Spec.Watermark.Disabled
	// for now only reduce has parallelism might have to modify later
	// checking parallelism for a vertex to identify reduce vertex
	// parallelism is only supported by reduce vertex for now else will be nil
	// parallelism indicates replica count ~ multiple pods for a vertex here
	replicas := int64(1)
	obj := ps.pipeline.GetFromEdges(vertexName)
	if len(obj) > 0 && obj[0].Parallelism != nil {
		replicas = int64(*obj[0].Parallelism)
	}

	// If watermark is not enabled, return time zero
	if ps.pipeline.Spec.Watermark.Disabled {
		timeZero := time.Unix(0, 0).UnixMilli()
		watermarks := make([]int64, replicas)
		for idx := range watermarks {
			watermarks[idx] = timeZero
		}
		v := &daemon.VertexWatermark{
			Pipeline:           &ps.pipeline.Name,
			Vertex:             request.Vertex,
			Watermarks:         watermarks,
			IsWatermarkEnabled: &isWatermarkEnabled,
		}
		resp.VertexWatermark = v
		return resp, nil
	}

	// Watermark is enabled
	fetchersKey := vertexName + "/" + strconv.FormatInt(replicas, 10)
	vertexFetchers, ok := ps.watermarkFetchers[fetchersKey]

	// Vertex not found
	if !ok {
		log.Errorf("Watermark fetchers not available for vertex %s in the fetcher map", vertexName)
		return nil, fmt.Errorf("watermark not available for given vertex, %s", vertexName)
	}

	var latestWatermark = int64(-1)
	var latestWatermarks []processor.Watermark
	for _, fetcher := range vertexFetchers {
		watermarks := fetcher.GetHeadWatermarks()
		sort.Slice(watermarks, func(i, j int) bool { return watermarks[i].UnixMilli() > watermarks[j].UnixMilli() })
		watermark := watermarks[0].UnixMilli()
		if watermark >= latestWatermark {
			latestWatermark = watermark
			latestWatermarks = watermarks
		}
	}
	var watermarks []int64
	for _, v := range latestWatermarks {
		watermarks = append(watermarks, v.UnixMilli())
	}
	v := &daemon.VertexWatermark{
		Pipeline:           &ps.pipeline.Name,
		Vertex:             request.Vertex,
		Watermarks:         watermarks,
		IsWatermarkEnabled: &isWatermarkEnabled,
	}
	resp.VertexWatermark = v
	return resp, nil
}

// GetPipelineWatermarks is used to return the head watermarks for a given pipeline.
func (ps *pipelineMetadataQuery) GetPipelineWatermarks(ctx context.Context, request *daemon.GetPipelineWatermarksRequest) (*daemon.GetPipelineWatermarksResponse, error) {
	resp := new(daemon.GetPipelineWatermarksResponse)
	isWatermarkEnabled := !ps.pipeline.Spec.Watermark.Disabled

	// If watermark is not enabled, return time zero
	if ps.pipeline.Spec.Watermark.Disabled {
		timeZero := time.Unix(0, 0).UnixMilli()
		watermarkArr := make([]*daemon.VertexWatermark, len(ps.watermarkFetchers))
		i := 0
		for k := range ps.watermarkFetchers {
			vertexName := strings.Split(k, "/")[0]
			replicas, _ := strconv.ParseInt(strings.Split(k, "/")[1], 10, 64)
			watermarks := make([]int64, replicas)
			for idx := range watermarks {
				watermarks[idx] = timeZero
			}
			watermarkArr[i] = &daemon.VertexWatermark{
				Pipeline:           &ps.pipeline.Name,
				Vertex:             &vertexName,
				Watermarks:         watermarks,
				IsWatermarkEnabled: &isWatermarkEnabled,
			}
			i++
		}
		resp.PipelineWatermarks = watermarkArr
		return resp, nil
	}

	// Watermark is enabled
	watermarkArr := make([]*daemon.VertexWatermark, len(ps.watermarkFetchers))
	i := 0
	for k, vertexFetchers := range ps.watermarkFetchers {
		var latestWatermark = int64(-1)
		var latestWatermarks []processor.Watermark
		for _, fetcher := range vertexFetchers {
			watermarks := fetcher.GetHeadWatermarks()
			sort.Slice(watermarks, func(i, j int) bool { return watermarks[i].UnixMilli() > watermarks[j].UnixMilli() })
			watermark := watermarks[0].UnixMilli()
			if watermark >= latestWatermark {
				latestWatermark = watermark
				latestWatermarks = watermarks
			}
		}
		vertexName := strings.Split(k, "/")[0]
		var watermarks []int64
		for _, v := range latestWatermarks {
			watermarks = append(watermarks, v.UnixMilli())
		}
		watermarkArr[i] = &daemon.VertexWatermark{
			Pipeline:           &ps.pipeline.Name,
			Vertex:             &vertexName,
			Watermarks:         watermarks,
			IsWatermarkEnabled: &isWatermarkEnabled,
		}
		i++
	}
	resp.PipelineWatermarks = watermarkArr
	return resp, nil
}
