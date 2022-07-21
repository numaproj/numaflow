// Package for Daemon based service in cluster
package service

import (
	"context"
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"go.uber.org/zap"
	"time"
)

// watermarkFetchers used to store watermark metadata for propagation
type watermarkFetchers struct {
	fetchMap map[string]fetch.Fetcher
}

// newVertexWatermarkFetcher creates a new instance of watermarkFetchers. This is used to populate a map of vertices to
// corresponding fetchers. These fetchers are tied to the incoming edge buffer of the current vertex (Vn), and read the
// watermark propagated by the vertex (Vn-1). As each vertex has one incoming edge, for the input vertex we read the source
// data buffer.
func newVertexWatermarkFetcher(pipeline *v1alpha1.Pipeline) *watermarkFetchers {
	ctx := context.Background()
	log := logging.FromContext(ctx)
	vertexWmMap := make(map[string]fetch.Fetcher)
	pipelineName := pipeline.Name
	var fromBufferName string
	for _, vertex := range pipeline.Spec.Vertices {
		// TODO: Checking if Vertex is source
		if vertex.Source != nil {
			fromBufferName = v1alpha1.GenerateSourceBufferName(pipeline.Namespace, pipelineName, vertex.Name)
		} else {
			// Currently we support only one incoming edge
			edge := pipeline.GetFromEdges(vertex.Name)[0]
			fromBufferName = v1alpha1.GenerateEdgeBufferName(pipeline.Namespace, pipelineName, edge.From, edge.To)
		}
		hbBucket := isbsvc.JetStreamProcessorBucket(pipelineName, fromBufferName)
		hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, hbBucket, clients.NewInClusterJetStreamClient())
		if err != nil {
			log.Fatalw("JetStreamKVWatch failed", zap.String("HeartbeatBucket", hbBucket), zap.Error(err))
		}
		otBucket := isbsvc.JetStreamOTBucket(pipelineName, fromBufferName)
		otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, otBucket, clients.NewInClusterJetStreamClient())
		if err != nil {
			log.Fatalw("JetStreamKVWatch failed", zap.String("OTBucket", otBucket), zap.Error(err))
		}
		var fetchWmWatchers = generic.BuildFetchWMWatchers(hbWatch, otWatch)
		fetchWatermark := generic.NewGenericFetch(ctx, vertex.Name, fetchWmWatchers)
		vertexWmMap[vertex.Name] = fetchWatermark
	}
	return &watermarkFetchers{
		fetchMap: vertexWmMap,
	}
}

// GetVertexWatermark is used to return the head watermark for a given vertex.
func (ps *pipelineMetricsQueryService) GetVertexWatermark(ctx context.Context, request *daemon.GetVertexWatermarkRequest) (*daemon.GetVertexWatermarkResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.GetVertexWatermarkResponse)
	vertexName := request.GetVertex()
	vertexFetcher := ps.vertexWatermark.fetchMap[vertexName]
	// Error case?
	if vertexFetcher == nil {
		log.Errorf("Fetcher not available")
	}
	vertexWatermark := time.Time(vertexFetcher.GetHeadWatermark()).Unix()
	v := &daemon.VertexWatermark{
		Pipeline:  &ps.pipeline.Name,
		Vertex:    request.Vertex,
		Watermark: &vertexWatermark,
	}
	resp.VertexWatermark = v
	return resp, nil
}
