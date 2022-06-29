package service

import (
	"context"
	"fmt"

	"k8s.io/utils/pointer"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type isbSvcQueryService struct {
	client   isbsvc.ISBService
	pipeline *v1alpha1.Pipeline
}

func NewISBSvcQueryService(client isbsvc.ISBService, pipeline *v1alpha1.Pipeline) *isbSvcQueryService {
	return &isbSvcQueryService{
		client:   client,
		pipeline: pipeline,
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
