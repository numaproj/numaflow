package isbsvc

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/redis"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type isbsRedisSvc struct {
	client *redisclient.RedisClient
}

// NewISBRedisSvc is used to return a new object of type isbsRedisSvc
func NewISBRedisSvc(client *redisclient.RedisClient) ISBService {
	return &isbsRedisSvc{client: client}
}

// CreateBuffers is used to create the inter-step redis buffers.
func (r *isbsRedisSvc) CreateBuffers(ctx context.Context, buffers []dfv1.Buffer, opts ...BufferCreateOption) error {
	log := logging.FromContext(ctx)
	failToCreate := false
	for _, s := range buffers {
		if s.Type != dfv1.EdgeBuffer {
			continue
		}
		stream := s.Name
		group := fmt.Sprintf("%s-group", stream)
		err := r.client.CreateStreamGroup(ctx, stream, group, redisclient.ReadFromEarliest)
		if err != nil {
			if redisclient.IsAlreadyExistError(err) {
				log.Warnw("Stream already exists.", zap.String("group", group), zap.String("stream", stream))
			} else {
				failToCreate = true
				log.Errorw("Failed to Redis Stream and Group creation.", zap.String("group", group), zap.String("stream", stream), zap.Error(err))
			}
		} else {
			log.Infow("Redis StreamGroup created", zap.String("group", group), zap.String("stream", stream))
		}
	}
	if failToCreate {
		return fmt.Errorf("failed all or some Streams creation")
	}
	return nil
}

// DeleteBuffers is used to delete the inter-step redis buffers.
func (r *isbsRedisSvc) DeleteBuffers(ctx context.Context, buffers []dfv1.Buffer) error {
	log := logging.FromContext(ctx)
	failToDelete := false
	streamNames := []string{}
	for _, s := range buffers {
		if s.Type != dfv1.EdgeBuffer {
			continue
		}
		stream := s.Name
		streamNames = append(streamNames, stream)
		group := fmt.Sprintf("%s-group", stream)
		if err := r.client.DeleteStreamGroup(ctx, stream, group); err != nil {
			if redisclient.NotFoundError(err) {
				log.Warnw("Redis Streams group is not found.", zap.String("group", group), zap.String("stream", stream))
			} else {
				failToDelete = true
				log.Errorw("Failed Redis StreamGroup deletion.", zap.String("group", group), zap.String("stream", stream), zap.Error(err))
			}
		} else {
			log.Infow("Redis StreamGroup deleted", zap.String("group", group), zap.String("stream", stream))
		}
	}
	if !failToDelete {
		log.Infow("Deleted Redis Streams groups successfully")
	}

	err := r.client.DeleteKeys(ctx, streamNames...)
	if err != nil {
		return err
	}
	log.Infow("Deleted Redis Streams successfully")

	if failToDelete {
		return fmt.Errorf("failed all or some Stream group deletion")
	}
	return nil
}

// ValidateBuffers is used to validate inter-step redis buffers to see if the stream/stream group exist
func (r *isbsRedisSvc) ValidateBuffers(ctx context.Context, buffers []dfv1.Buffer) error {
	for _, stream := range buffers {
		if stream.Type != dfv1.EdgeBuffer {
			continue
		}
		if !r.client.IsStreamExists(ctx, stream.Name) {
			return fmt.Errorf("stream %s not existing", stream.Name)
		}
		group := fmt.Sprintf("%s-group", stream.Name)
		if !r.client.IsStreamGroupExists(ctx, stream.Name, group) {
			return fmt.Errorf("group %s not existing", group)
		}
	}
	return nil
}

// GetBufferInfo is used to provide buffer information like pending count, buffer length, has unprocessed data etc.
func (r *isbsRedisSvc) GetBufferInfo(ctx context.Context, buffer dfv1.Buffer) (*BufferInfo, error) {
	if buffer.Type != dfv1.EdgeBuffer {
		return nil, fmt.Errorf("buffer infomation inquiry is not supported for type %q", buffer.Type)
	}
	group := fmt.Sprintf("%s-group", buffer.Name)
	rqw := redis.NewBufferWrite(ctx, redisclient.NewInClusterRedisClient(), buffer.Name, group, redis.WithRefreshBufferWriteInfo(false))
	var bufferWrite = rqw.(*redis.BufferWrite)

	bufferInfo := &BufferInfo{
		Name:            buffer.Name,
		PendingCount:    bufferWrite.GetPendingCount(),
		AckPendingCount: 0,                             // TODO: this should not be 0
		TotalMessages:   bufferWrite.GetPendingCount(), // TODO: what should this be?
	}

	return bufferInfo, nil
}
