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

package watermark

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// HTTPWatermarkService manages HTTP-based watermark fetching for all edges in a pipeline
type HTTPWatermarkService struct {
	pipeline *v1alpha1.Pipeline
	fetchers map[string]*HTTPWatermarkFetcher // key is edge name (from-to)
	log      *zap.SugaredLogger
}

// NewHTTPWatermarkService creates a new HTTP watermark service
func NewHTTPWatermarkService(ctx context.Context, pipeline *v1alpha1.Pipeline) *HTTPWatermarkService {
	log := logging.FromContext(ctx).With("pipeline", pipeline.Name)

	service := &HTTPWatermarkService{
		pipeline: pipeline,
		fetchers: make(map[string]*HTTPWatermarkFetcher),
		log:      log,
	}

	// Create fetchers for all edges - cache by edge name
	for _, edge := range pipeline.ListAllEdges() {
		edgeName := edge.GetEdgeName()
		service.fetchers[edgeName] = NewHTTPWatermarkFetcher(ctx, pipeline, edge)
	}

	log.Infof("Created HTTP watermark service with %d edge fetchers", len(service.fetchers))
	return service
}

// GetPipelineWatermarks fetches watermarks for all edges in the pipeline
func (s *HTTPWatermarkService) GetPipelineWatermarks(ctx context.Context, _request *daemon.GetPipelineWatermarksRequest) (*daemon.GetPipelineWatermarksResponse, error) {
	resp := new(daemon.GetPipelineWatermarksResponse)
	isWatermarkEnabled := !s.pipeline.Spec.Watermark.Disabled

	// If watermark is not enabled, return time zero
	if s.pipeline.Spec.Watermark.Disabled {
		timeZero := time.Unix(0, 0).UnixMilli()
		watermarkArr := make([]*daemon.EdgeWatermark, len(s.fetchers))
		i := 0
		for edgeName, fetcher := range s.fetchers {
			toVertex := s.pipeline.GetVertex(fetcher.edge.To)
			partitionCount := toVertex.GetPartitionCount()

			watermarks := make([]*wrapperspb.Int64Value, partitionCount)
			for idx := range watermarks {
				watermarks[idx] = wrapperspb.Int64(timeZero)
			}

			watermarkArr[i] = &daemon.EdgeWatermark{
				Pipeline:           s.pipeline.Name,
				Edge:               edgeName,
				Watermarks:         watermarks,
				IsWatermarkEnabled: wrapperspb.Bool(isWatermarkEnabled),
				From:               fetcher.edge.From,
				To:                 fetcher.edge.To,
			}
			i++
		}
		resp.PipelineWatermarks = watermarkArr
		return resp, nil
	}

	// Watermark is enabled - serve from cache (background fetching keeps it updated)
	watermarkArr := make([]*daemon.EdgeWatermark, len(s.fetchers))
	i := 0
	for edgeName, fetcher := range s.fetchers {
		s.log.Debugf("Getting cached watermarks for edge: %s", edgeName)

		watermarks, err := fetcher.GetWatermarks()
		if err != nil {
			s.log.Errorw("Failed to get watermarks for edge",
				zap.String("edge", edgeName),
				zap.Error(err))
			// Continue with other edges even if one fails
			continue
		}

		watermarkArr[i] = &daemon.EdgeWatermark{
			Pipeline:           s.pipeline.Name,
			Edge:               edgeName,
			Watermarks:         watermarks,
			IsWatermarkEnabled: wrapperspb.Bool(isWatermarkEnabled),
			From:               fetcher.edge.From,
			To:                 fetcher.edge.To,
		}
		i++
	}

	// Resize the array to remove any failed fetches
	if i < len(watermarkArr) {
		watermarkArr = watermarkArr[:i]
	}

	resp.PipelineWatermarks = watermarkArr
	return resp, nil
}

// Stop stops all background fetching and cleans up resources
func (s *HTTPWatermarkService) Stop() {
	s.log.Info("Stopping HTTP watermark service")

	for edgeName, fetcher := range s.fetchers {
		s.log.Debugf("Stopping fetcher for edge: %s", edgeName)
		fetcher.Stop()
	}

	s.log.Info("All HTTP watermark fetchers stopped")
}
