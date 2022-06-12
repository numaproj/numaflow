package service

import (
	"context"
	"fmt"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"k8s.io/utils/pointer"
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
	for _, buffer := range is.pipeline.GetAllBuffers() {
		if buffer.Type != v1alpha1.EdgeBuffer {
			continue
		}
		bufferInfo, err := is.client.GetBufferInfo(ctx, buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to get information of buffer %q", buffer)
		}
		log.Debugf("Buffer %s has bufferInfo %+v", buffer, bufferInfo)
		vFrom, vTo := is.pipeline.FindVerticesWithEdgeBuffer(buffer.Name)
		if vFrom == nil || vTo == nil {
			return nil, fmt.Errorf("buffer %q not found from the pipeline", buffer)
		}
		bufferLength, bufferUsageLimit := getVertexLimits(is.pipeline, vFrom)
		usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
		if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
			usage = x
		}
		b := &daemon.BufferInfo{
			Pipeline:         &is.pipeline.Name,
			FromVertex:       &vFrom.Name,
			ToVertex:         &vTo.Name,
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
	vFrom, vTo := is.pipeline.FindVerticesWithEdgeBuffer(*req.Buffer)
	if vFrom == nil || vTo == nil {
		return nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", *req.Buffer)
	}
	bufferLength, bufferUsageLimit := getVertexLimits(is.pipeline, vFrom)
	usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
	if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
		usage = x
	}
	b := &daemon.BufferInfo{
		Pipeline:         &is.pipeline.Name,
		FromVertex:       &vFrom.Name,
		ToVertex:         &vTo.Name,
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

func getVertexLimits(pl *v1alpha1.Pipeline, v *v1alpha1.AbstractVertex) (bufferLength int64, bufferUsageLimit float64) {
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
	if x := v.Limits; x != nil {
		if x.BufferMaxLength != nil {
			bufferLength = int64(*x.BufferMaxLength)
		}
		if x.BufferUsageLimit != nil {
			bufferUsageLimit = float64(*x.BufferUsageLimit) / 100
		}
	}
	return bufferLength, bufferUsageLimit
}
