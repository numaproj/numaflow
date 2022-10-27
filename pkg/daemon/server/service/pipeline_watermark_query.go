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

// watermarkFetchers used to store watermark metadata for propagation
type watermarkFetchers struct {
	// A map used to query watermark for vertices
	// Key: vertex name
	// Value: a list of watermark fetchers, each fetcher represents one of the out edges for the vertex
	fetcherMap map[string][]fetch.Fetcher
}

// newVertexWatermarkFetcher creates a new instance of watermarkFetchers. This is used to populate a map of vertices to
// corresponding fetchers. The fetchers are to retrieve vertex level watermarks.
func newVertexWatermarkFetcher(pipeline *v1alpha1.Pipeline, isbSvcClient isbsvc.ISBService) (*watermarkFetchers, error) {
	ctx := context.Background()
	var wmFetcher = new(watermarkFetchers)

	if pipeline.Spec.Watermark.Disabled {
		return wmFetcher, nil
	}

	vertexToFetchersMap := make(map[string][]fetch.Fetcher)
	pipelineName := pipeline.Name

	// TODO: https://github.com/numaproj/numaflow/pull/120#discussion_r927316015
	for _, vertex := range pipeline.Spec.Vertices {
		if vertex.Sink != nil {
			toBufferName := v1alpha1.GenerateSinkBufferName(pipeline.Namespace, pipelineName, vertex.Name)
			fetchWatermark, err := isbSvcClient.CreateWatermarkFetcher(ctx, toBufferName)
			if err != nil {
				return nil, fmt.Errorf("failed to create watermark fetcher  %w", err)
			}
			vertexToFetchersMap[vertex.Name] = []fetch.Fetcher{fetchWatermark}
		} else {
			// If the vertex is not a sink, to fetch the watermark, we consult all out edges and grab the latest watermark among them.
			var wmFetcherList []fetch.Fetcher
			for _, edge := range pipeline.GetToEdges(vertex.Name) {
				toBufferNames := v1alpha1.GenerateEdgeBufferNames(pipeline.Namespace, pipelineName, edge)
				for _, toBufferName := range toBufferNames {
					fetchWatermark, err := isbSvcClient.CreateWatermarkFetcher(ctx, toBufferName)
					if err != nil {
						return nil, fmt.Errorf("failed to create watermark fetcher  %w", err)
					}
					wmFetcherList = append(wmFetcherList, fetchWatermark)
				}
			}
			vertexToFetchersMap[vertex.Name] = wmFetcherList
		}
	}
	wmFetcher.fetcherMap = vertexToFetchersMap
	return wmFetcher, nil
}

// GetVertexWatermark is used to return the head watermark for a given vertex.
func (ps *pipelineMetadataQuery) GetVertexWatermark(ctx context.Context, request *daemon.GetVertexWatermarkRequest) (*daemon.GetVertexWatermarkResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.GetVertexWatermarkResponse)
	vertexName := request.GetVertex()
	retFalse := false
	retTrue := true

	// If watermark is not enabled, return time zero
	if ps.pipeline.Spec.Watermark.Disabled {
		timeZero := time.Unix(0, 0).UnixMilli()
		v := &daemon.VertexWatermark{
			Pipeline:           &ps.pipeline.Name,
			Vertex:             request.Vertex,
			Watermark:          &timeZero,
			IsWatermarkEnabled: &retFalse,
		}
		resp.VertexWatermark = v
		return resp, nil
	}

	// Watermark is enabled
	vertexFetchers, ok := ps.vertexWatermark.fetcherMap[vertexName]

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
		IsWatermarkEnabled: &retTrue,
	}
	resp.VertexWatermark = v
	return resp, nil
}
