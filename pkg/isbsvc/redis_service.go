package isbsvc

import (
	"context"
	"fmt"
	redis2 "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
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
		stream := redisclient.GetRedisStreamName(s.Name)
		group := fmt.Sprintf("%s-group", s.Name)
		err := r.client.CreateStreamGroup(ctx, stream, group, redisclient.ReadFromEarliest)
		if err != nil {
			if redisclient.IsAlreadyExistError(err) {
				log.Warnw("Stream already exists.", zap.String("group", group), zap.String("stream", stream))
			} else {
				failToCreate = true
				log.Errorw("Failed to create Redis Stream and Group.", zap.String("group", group), zap.String("stream", stream), zap.Error(err))
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
	// FIXME: delete the keys created by the lua script
	log := logging.FromContext(ctx)
	var errList error
	for _, s := range buffers {
		if s.Type != dfv1.EdgeBuffer {
			continue
		}
		stream := redisclient.GetRedisStreamName(s.Name)
		group := fmt.Sprintf("%s-group", s.Name)
		if err := r.client.DeleteStreamGroup(ctx, stream, group); err != nil {
			if redisclient.NotFoundError(err) {
				log.Warnw("Redis StreamGroup is not found.", zap.String("group", group), zap.String("stream", stream))
			} else {
				errList = multierr.Append(errList, err)
				log.Errorw("Failed to delete Redis StreamGroup.", zap.String("group", group), zap.String("stream", stream), zap.Error(err))
			}
		} else {
			log.Infow("Redis StreamGroup deleted", zap.String("group", group), zap.String("stream", stream))
		}

		if err := r.client.DeleteKeys(ctx, stream); err != nil {
			errList = multierr.Append(errList, err)
			log.Errorw("Failed to delete Redis keys.", zap.String("stream", stream), zap.Error(err))
		} else {
			log.Infow("Redis keys deleted", zap.String("stream", stream))
		}
	}
	if errList != nil {
		return fmt.Errorf("failed to delete all or some Redis StreamGroups and keys")
	}
	log.Infow("Deleted Redis StreamGroups and keys successfully")
	return nil
}

// ValidateBuffers is used to validate inter-step redis buffers to see if the stream/stream group exist
func (r *isbsRedisSvc) ValidateBuffers(ctx context.Context, buffers []dfv1.Buffer) error {
	for _, s := range buffers {
		if s.Type != dfv1.EdgeBuffer {
			continue
		}
		var stream = redisclient.GetRedisStreamName(s.Name)
		if !r.client.IsStreamExists(ctx, stream) {
			return fmt.Errorf("s %s not existing", stream)
		}
		group := fmt.Sprintf("%s-group", s.Name)
		if !r.client.IsStreamGroupExists(ctx, stream, group) {
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
	rqw := redis2.NewBufferWrite(ctx, redisclient.NewInClusterRedisClient(), buffer.Name, group, redis2.WithRefreshBufferWriteInfo(false))
	var bufferWrite = rqw.(*redis2.BufferWrite)

	bufferInfo := &BufferInfo{
		Name:            buffer.Name,
		PendingCount:    bufferWrite.GetPendingCount(),
		AckPendingCount: 0,                             // TODO: this should not be 0
		TotalMessages:   bufferWrite.GetPendingCount(), // TODO: what should this be?
	}

	return bufferInfo, nil
}

func (r *isbsRedisSvc) CreateWatermarkFetcher(ctx context.Context, bufferName string) (fetch.Fetcher, error) {
	// Watermark fetching is not supported for Redis ATM. Creating noop watermark fetcher.
	hbWatcher := noop.NewKVOpWatch()
	otWatcher := noop.NewKVOpWatch()
	watermarkFetcher := generic.NewGenericEdgeFetch(ctx, bufferName, store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher))
	return watermarkFetcher, nil
}
