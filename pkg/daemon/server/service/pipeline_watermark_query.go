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
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
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
		if vertex.Sink != nil {
			toBufferName := v1alpha1.GenerateSinkBufferName(pipeline.Namespace, pipeline.Name, vertex.Name)
			wmFetcher, err := isbSvcClient.CreateWatermarkFetcher(ctx, toBufferName)
			if err != nil {
				return nil, fmt.Errorf("failed to create watermark fetcher, %w", err)
			}
			wmFetchers[vertex.Name] = []fetch.Fetcher{wmFetcher}
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
			wmFetchers[vertex.Name] = wmFetcherList
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

	// If watermark is not enabled, return time zero
	if ps.pipeline.Spec.Watermark.Disabled {
		timeZero := time.Unix(0, 0).UnixMilli()
		v := &daemon.VertexWatermark{
			Pipeline:           &ps.pipeline.Name,
			Vertex:             request.Vertex,
			Watermark:          &timeZero,
			IsWatermarkEnabled: &isWatermarkEnabled,
		}
		resp.VertexWatermark = v
		return resp, nil
	}

	// Watermark is enabled
	vertexFetchers, ok := ps.watermarkFetchers[vertexName]

	// Vertex not found
	if !ok {
		log.Errorf("Watermark fetchers not available for vertex %s in the fetcher map", vertexName)
		return nil, fmt.Errorf("watermark not available for given vertex, %s", vertexName)
	}

	var latestWatermark = int64(-1)
	for _, fetcher := range vertexFetchers {
		watermark := fetcher.GetHeadWatermark().UnixMilli()
		if watermark > latestWatermark {
			latestWatermark = watermark
		}
	}

	v := &daemon.VertexWatermark{
		Pipeline:           &ps.pipeline.Name,
		Vertex:             request.Vertex,
		Watermark:          &latestWatermark,
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
			vertexName := k
			watermarkArr[i] = &daemon.VertexWatermark{
				Pipeline:           &ps.pipeline.Name,
				Vertex:             &vertexName,
				Watermark:          &timeZero,
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
		for _, fetcher := range vertexFetchers {
			watermark := fetcher.GetHeadWatermark().UnixMilli()
			if watermark > latestWatermark {
				latestWatermark = watermark
			}
		}
		vertexName := k
		watermarkArr[i] = &daemon.VertexWatermark{
			Pipeline:           &ps.pipeline.Name,
			Vertex:             &vertexName,
			Watermark:          &latestWatermark,
			IsWatermarkEnabled: &isWatermarkEnabled,
		}
		i++
	}
	resp.PipelineWatermarks = watermarkArr
	return resp, nil
}
