package isbsvc

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type isbsRedisSvc struct {
	client *clients.RedisClient
}

// NewISBRedisSvc is used to return a new object of type isbsRedisSvc
func NewISBRedisSvc(client *clients.RedisClient) ISBService {
	return &isbsRedisSvc{client: client}
}

// CreateBuffers is used to create the inter-step redis buffers.
func (r *isbsRedisSvc) CreateBuffers(ctx context.Context, buffers []string, opts ...BufferCreateOption) error {
	log := logging.FromContext(ctx)
	failToCreate := false
	for _, stream := range buffers {
		group := fmt.Sprintf("%s-group", stream)
		err := r.client.CreateStreamGroup(ctx, stream, group, clients.ReadFromEarliest)
		if err != nil {
			if clients.IsAlreadyExistError(err) {
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
func (r *isbsRedisSvc) DeleteBuffers(ctx context.Context, buffers []string) error {
	log := logging.FromContext(ctx)
	failToDelete := false
	for _, stream := range buffers {
		group := fmt.Sprintf("%s-group", stream)
		if err := r.client.DeleteStreamGroup(ctx, stream, group); err != nil {
			if clients.NotFoundError(err) {
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

	err := r.client.DeleteKeys(ctx, buffers...)
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
func (r *isbsRedisSvc) ValidateBuffers(ctx context.Context, buffers []string) error {
	for _, streamName := range buffers {
		if !r.client.IsStreamExists(ctx, streamName) {
			return fmt.Errorf("stream %s not existing", streamName)
		}
		group := fmt.Sprintf("%s-group", streamName)
		if !r.client.IsStreamGroupExists(ctx, streamName, group) {
			return fmt.Errorf("group %s not existing", group)
		}
	}
	return nil
}

// GetBufferInfo is used to provide buffer information like pending count, buffer length, has unprocessed data etc.
func (r *isbsRedisSvc) GetBufferInfo(ctx context.Context, stream string) (*BufferInfo, error) {
	group := fmt.Sprintf("%s-group", stream)
	rqw := redis.NewBufferWrite(ctx, clients.NewInClusterRedisClient(), stream, group, redis.WithRefreshBufferWriteInfo(false))
	var bufferWrite = rqw.(*redis.BufferWrite)

	bufferInfo := &BufferInfo{
		Name:            stream,
		PendingCount:    bufferWrite.GetPendingCount(),
		AckPendingCount: 0,                             // TODO: this should not be 0
		TotalMessages:   bufferWrite.GetPendingCount(), // TODO: what should this be?
	}

	return bufferInfo, nil
}
