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

package isbsvc

import (
	"context"
	"fmt"

	redis2 "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
	"go.uber.org/multierr"
	"go.uber.org/zap"

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
func (r *isbsRedisSvc) CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, opts ...CreateOption) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	log := logging.FromContext(ctx)
	failToCreate := false
	for _, s := range buffers {
		stream := redisclient.GetRedisStreamName(s)
		group := fmt.Sprintf("%s-group", s)
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
func (r *isbsRedisSvc) DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	// FIXME: delete the keys created by the lua script
	log := logging.FromContext(ctx)
	var errList error
	for _, s := range buffers {
		stream := redisclient.GetRedisStreamName(s)
		group := fmt.Sprintf("%s-group", s)
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
func (r *isbsRedisSvc) ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	for _, s := range buffers {
		var stream = redisclient.GetRedisStreamName(s)
		if !r.client.IsStreamExists(ctx, stream) {
			return fmt.Errorf("s %s not existing", stream)
		}
		group := fmt.Sprintf("%s-group", s)
		if !r.client.IsStreamGroupExists(ctx, stream, group) {
			return fmt.Errorf("group %s not existing", group)
		}
	}
	return nil
}

// GetBufferInfo is used to provide buffer information like pending count, buffer length, has unprocessed data etc.
func (r *isbsRedisSvc) GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error) {
	group := fmt.Sprintf("%s-group", buffer)
	rqw := redis2.NewBufferWrite(ctx, redisclient.NewInClusterRedisClient(), buffer, group, redisclient.WithRefreshBufferWriteInfo(false))
	var bufferWrite = rqw.(*redis2.BufferWrite)

	bufferInfo := &BufferInfo{
		Name:            buffer,
		PendingCount:    bufferWrite.GetPendingCount(),
		AckPendingCount: 0,                             // TODO: this should not be 0
		TotalMessages:   bufferWrite.GetPendingCount(), // TODO: what should this be?
	}

	return bufferInfo, nil
}

func (r *isbsRedisSvc) CreateWatermarkFetcher(ctx context.Context, bucketName string) (fetch.Fetcher, error) {
	// Watermark fetching is not supported for Redis ATM. Creating noop watermark fetcher.
	hbWatcher := noop.NewKVOpWatch()
	otWatcher := noop.NewKVOpWatch()
	storeWatcher := store.BuildWatermarkStoreWatcher(hbWatcher, otWatcher)
	pm := fetch.NewProcessorManager(ctx, storeWatcher)
	watermarkFetcher := fetch.NewEdgeFetcher(ctx, bucketName, storeWatcher, pm)
	return watermarkFetcher, nil
}
