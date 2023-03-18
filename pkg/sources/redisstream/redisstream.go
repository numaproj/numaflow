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

package redisstream

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"go.uber.org/zap"
)

type redisStreamsSource struct {
	*redisclient.RedisStreamsReader
	// name of the pipeline
	pipelineName string
	// forwarder that writes the consumed data to destination
	forwarder *forward.InterStepDataForward
	// context cancel function
	cancelfn context.CancelFunc
	// source watermark publisher
	sourcePublishWM publish.Publisher
}

type Option func(*redisStreamsSource) error //todo: implement these

func New(
	vertexInstance *dfv1.VertexInstance,
	writers []isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	mapApplier applier.MapApplier,
	fetchWM fetch.Fetcher,
	publishWM map[string]publish.Publisher,
	publishWMStores store.WatermarkStorer,
	opts ...Option) (*redisStreamsSource, error) {

	// create RedisClient and create RedisStreamsReader passing that in
	vertexSpec := vertexInstance.Vertex.Spec
	redisSpec := vertexSpec.Source.RedisStreamsSource
	redisClient := newRedisClient(redisSpec)

	readerOpts := &redisclient.Options{ //TODO: do we want read timeout like is an option in common.go?
		InfoRefreshInterval: time.Second,
		ReadTimeOut:         time.Second,
		CheckBackLog:        true,
	}

	redisStreamsReader := &redisclient.RedisStreamsReader{
		Name:        vertexSpec.Name,
		Stream:      redisclient.GetRedisStreamName(vertexSpec.Name),
		Group:       redisSpec.ConsumerGroupName,
		Consumer:    fmt.Sprintf("%s-%v", vertexSpec.Name, vertexInstance.Replica),
		RedisClient: redisClient,
		Options:     *readerOpts,
		Metrics: redisclient.Metrics{
			ReadErrors: redisStreamsSourceReadErrors,
			Reads:      redisStreamsSourceReadCount,
			Acks:       redisStreamsSourceAckCount,
		},
	}
	redisStreamsReader.Log = logging.NewLogger()

	redisStreamsSource := &redisStreamsSource{
		RedisStreamsReader: redisStreamsReader,
		pipelineName:       vertexSpec.PipelineName,
	}

	destinations := make(map[string]isb.BufferWriter, len(writers))
	for _, w := range writers {
		destinations[w.GetName()] = w
	}
	forwardOpts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeSource), forward.WithLogger(redisStreamsSource.Log), forward.WithSourceWatermarkPublisher(redisStreamsSource)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := forward.NewInterStepDataForward(vertexInstance.Vertex, redisStreamsSource, destinations, fsd, mapApplier, fetchWM, publishWM, forwardOpts...)
	if err != nil {
		redisStreamsSource.Log.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	redisStreamsSource.forwarder = forwarder
	ctx, cancel := context.WithCancel(context.Background())
	redisStreamsSource.cancelfn = cancel
	entityName := fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	processorEntity := processor.NewProcessorEntity(entityName)
	redisStreamsSource.sourcePublishWM = publish.NewPublish(ctx, processorEntity, publishWMStores, publish.IsSource(), publish.WithDelay(vertexInstance.Vertex.Spec.Watermark.GetMaxDelay()))

	// todo: add TLS, auth

	// create the ConsumerGroup here if not already created: it's okay if this fails
	err = redisClient.CreateStreamGroup(ctx, redisStreamsReader.Stream, redisStreamsReader.Group, redisclient.ReadFromLatest)
	// TODO: if err != alreadyCreated { return err }

	return redisStreamsSource, nil
}

func newRedisClient(sourceSpec *dfv1.RedisStreamsSource) *redisclient.RedisClient {
	opts := &redis.UniversalOptions{
		Addrs: strings.Split(sourceSpec.URLs, ","), //todo: what is Sentinel and should I enable it?
		//Username:  //todo: add auth
		//Password:
		// MaxRedirects is an option for redis cluster mode.
		// The default value is set 3 to allow redirections when using redis cluster mode.
		// ref: if we use redis cluster client directly instead of redis universal client, the default value is 3
		//      https://github.com/go-redis/redis/blob/f6a8adc50cdaec30527f50d06468f9176ee674fe/cluster.go#L33-L36
		MaxRedirects: 3,
	}
	return redisclient.NewRedisClient(opts)
}

func (rsSource *redisStreamsSource) PublishSourceWatermarks(msgs []*isb.ReadMessage) {
	var oldest time.Time
	for _, m := range msgs {
		if oldest.IsZero() || m.EventTime.Before(oldest) {
			oldest = m.EventTime
		}
	}
	if len(msgs) > 0 && !oldest.IsZero() {
		rsSource.sourcePublishWM.PublishWatermark(wmb.Watermark(oldest), nil) // Source publisher does not care about the offset
	}
}

func (rsSource *redisStreamsSource) Close() error {
	rsSource.Log.Info("Shutting down nats source server...")
	rsSource.cancelfn()
	if err := rsSource.sourcePublishWM.Close(); err != nil {
		rsSource.Log.Errorw("Failed to close source vertex watermark publisher", zap.Error(err))
	}
	rsSource.Log.Info("Nats source server shutdown")
	return nil
}

func (rsSource *redisStreamsSource) Stop() {
	rsSource.Log.Info("Stopping redis streams source reader...")
	rsSource.forwarder.Stop()
}

func (rsSource *redisStreamsSource) ForceStop() {
	rsSource.Stop()
}

func (rsSource *redisStreamsSource) Start() <-chan struct{} {
	return rsSource.forwarder.Start()
}
