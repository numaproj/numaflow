package service

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strconv"

	"github.com/prometheus/common/expfmt"
	"k8s.io/utils/pointer"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// HTTPClient interface for the GET call to metrics endpoint. Had to add this an interface for testing
type HTTPClient interface {
	Get(url string) (*http.Response, error)
}

type isbSvcQueryService struct {
	client     isbsvc.ISBService
	pipeline   *v1alpha1.Pipeline
	httpClient HTTPClient
}

// NewISBSvcQueryService returns a new instance of isbSvcQueryService
func NewISBSvcQueryService(client isbsvc.ISBService, pipeline *v1alpha1.Pipeline) *isbSvcQueryService {
	return &isbSvcQueryService{
		client:   client,
		pipeline: pipeline,
		httpClient: &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}},
	}
}

// ListBuffers is used to obtain the all the edge buffers information of a pipeline
func (is *isbSvcQueryService) ListBuffers(ctx context.Context, req *daemon.ListBuffersRequest) (*daemon.ListBuffersResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.ListBuffersResponse)

	buffers := []*daemon.BufferInfo{}
	for _, edge := range is.pipeline.Spec.Edges {
		buffer := v1alpha1.GenerateEdgeBufferName(is.pipeline.Namespace, is.pipeline.Name, edge.From, edge.To)
		bufferInfo, err := is.client.GetBufferInfo(ctx, v1alpha1.Buffer{Name: buffer, Type: v1alpha1.EdgeBuffer})
		if err != nil {
			return nil, fmt.Errorf("failed to get information of buffer %q", buffer)
		}
		log.Debugf("Buffer %s has bufferInfo %+v", buffer, bufferInfo)
		bufferLength, bufferUsageLimit := getBufferLimits(is.pipeline, edge)
		usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
		if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
			usage = x
		}
		b := &daemon.BufferInfo{
			Pipeline:         &is.pipeline.Name,
			FromVertex:       pointer.String(fmt.Sprintf("%v", edge.From)),
			ToVertex:         pointer.String(fmt.Sprintf("%v", edge.To)),
			BufferName:       pointer.String(fmt.Sprintf("%v", buffer)),
			PendingCount:     &bufferInfo.PendingCount,
			AckPendingCount:  &bufferInfo.AckPendingCount,
			TotalMessages:    &bufferInfo.TotalMessages,
			BufferLength:     &bufferLength,
			BufferUsageLimit: &bufferUsageLimit,
			BufferUsage:      &usage,
			IsFull:           pointer.Bool(usage >= bufferUsageLimit),
		}
		buffers = append(buffers, b)
	}
	resp.Buffers = buffers
	return resp, nil
}

// GetBuffer is used to obtain one buffer information of a pipeline
func (is *isbSvcQueryService) GetBuffer(ctx context.Context, req *daemon.GetBufferRequest) (*daemon.GetBufferResponse, error) {
	bufferInfo, err := is.client.GetBufferInfo(ctx, v1alpha1.Buffer{Name: *req.Buffer, Type: v1alpha1.EdgeBuffer})
	if err != nil {
		return nil, fmt.Errorf("failed to get information of buffer %q", *req.Buffer)
	}
	edge := is.pipeline.FindEdgeWithBuffer(*req.Buffer)
	if edge == nil {
		return nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", *req.Buffer)
	}
	bufferLength, bufferUsageLimit := getBufferLimits(is.pipeline, *edge)
	usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
	if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
		usage = x
	}
	b := &daemon.BufferInfo{
		Pipeline:         &is.pipeline.Name,
		FromVertex:       &edge.From,
		ToVertex:         &edge.To,
		BufferName:       req.Buffer,
		PendingCount:     &bufferInfo.PendingCount,
		AckPendingCount:  &bufferInfo.AckPendingCount,
		TotalMessages:    &bufferInfo.TotalMessages,
		BufferLength:     &bufferLength,
		BufferUsageLimit: &bufferUsageLimit,
		BufferUsage:      &usage,
		IsFull:           pointer.Bool(usage >= bufferUsageLimit),
	}
	resp := new(daemon.GetBufferResponse)
	resp.Buffer = b
	return resp, nil
}

// GetVertexInfo is used to query the metrics service and is used to obtain the processing rate of a given vertex for 1m, 5m and 15m.
// In the future maybe latency will also be added here?
// Should this method live here or maybe another file?
func (is *isbSvcQueryService) GetVertexInfo(ctx context.Context, req *daemon.GetVertexRequest) (*daemon.GetVertexResponse, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.GetVertexResponse)

	metricsPort := strconv.Itoa(v1alpha1.VertexMetricsPort)
	pipelineVertex := fmt.Sprintf("%s-%s", is.pipeline.Name, *req.Vertex)
	// We can query the metrics endpoint of the 0th pod to obtain this value.
	// example: https://simple-pipeline-in-0.simple-pipeline-headless.svc.cluster.local:2469/metrics
	url := fmt.Sprintf("https://%s-0.%s-headless.%s.svc.cluster.local:%s/metrics", pipelineVertex, pipelineVertex, *req.Namespace, metricsPort)

	res, err := is.httpClient.Get(url)

	if err != nil {
		log.Errorf("Error reading the metrics endpoint: %s", err.Error())
		return nil, err
	}

	// expfmt Parser from prometheus to parse the metrics
	textParser := expfmt.TextParser{}
	result, err := textParser.TextToMetricFamilies(res.Body)
	if err != nil {
		log.Errorf("Error in parsing to prometheus metric families: %s", err.Error())
		return nil, err
	}

	processingRates := make([]*daemon.ProcessingRate, 0)
	// Check if the resultant metrics list contains the processingRate, if it does look for the period label
	if value, ok := result[v1alpha1.VertexProcessingRate]; ok {
		metrics := value.GetMetric()
		for _, metric := range metrics {
			labels := metric.GetLabel()
			for _, label := range labels {
				if label.GetName() == v1alpha1.MetricPeriodLabel {
					lookback := label.GetValue()
					rate := float32(metric.Gauge.GetValue())
					processingRate := &daemon.ProcessingRate{
						Lookback: &lookback,
						Rate:     &rate,
					}
					processingRates = append(processingRates, processingRate)
				}
			}
		}
	}
	v := &daemon.VertexInfo{
		Pipeline:       &is.pipeline.Name,
		Vertex:         req.Vertex,
		ProcessingRate: processingRates,
	}
	resp.Vertex = v
	return resp, nil
}

func getBufferLimits(pl *v1alpha1.Pipeline, edge v1alpha1.Edge) (bufferLength int64, bufferUsageLimit float64) {
	bufferLength = int64(v1alpha1.DefaultBufferLength)
	bufferUsageLimit = v1alpha1.DefaultBufferUsageLimit
	if x := pl.Spec.Limits; x != nil {
		if x.BufferMaxLength != nil {
			bufferLength = int64(*x.BufferMaxLength)
		}
		if x.BufferUsageLimit != nil {
			bufferUsageLimit = float64(*x.BufferUsageLimit) / 100
		}
	}
	if x := edge.Limits; x != nil {
		if x.BufferMaxLength != nil {
			bufferLength = int64(*x.BufferMaxLength)
		}
		if x.BufferUsageLimit != nil {
			bufferUsageLimit = float64(*x.BufferUsageLimit) / 100
		}
	}
	return bufferLength, bufferUsageLimit
}
