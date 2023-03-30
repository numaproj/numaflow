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

package redisstreams

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"go.uber.org/zap"
)

type redisStreamsSource struct {
	// shared code to read from Redis Streams
	*redisclient.RedisStreamsRead
	// name of the pipeline
	pipelineName string
	// forwarder that writes the consumed data to destination
	forwarder *forward.InterStepDataForward
	// context cancel function
	cancelfn context.CancelFunc
	// source watermark publisher
	sourcePublishWM publish.Publisher
}

type Option func(*redisStreamsSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *redisStreamsSource) error {
		o.Log = l
		return nil
	}
}

// WithReadTimeOut sets the read timeout
func WithReadTimeOut(t time.Duration) Option {
	return func(o *redisStreamsSource) error {
		o.Options.ReadTimeOut = t
		return nil
	}
}

func New(
	vertexInstance *dfv1.VertexInstance,
	writers []isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	mapApplier applier.MapApplier,
	fetchWM fetch.Fetcher,
	publishWM map[string]publish.Publisher,
	publishWMStores store.WatermarkStorer,
	opts ...Option) (*redisStreamsSource, error) {

	// create RedisClient to connect to Redis
	vertexSpec := vertexInstance.Vertex.Spec
	redisSpec := vertexSpec.Source.RedisStreams
	redisClient, err := newRedisClient(redisSpec)
	if err != nil {
		return nil, err
	}

	readerOpts := &redisclient.Options{
		InfoRefreshInterval: time.Second,
		ReadTimeOut:         time.Second,
		CheckBackLog:        true,
	}

	// RedisStreamsReader handles reading from Redis Streams
	redisStreamsReader := &redisclient.RedisStreamsRead{
		Name:        vertexSpec.Name,
		Stream:      redisSpec.Stream,
		Group:       redisSpec.ConsumerGroup,
		Consumer:    fmt.Sprintf("%s-%v", vertexSpec.Name, vertexInstance.Replica),
		RedisClient: redisClient,
		Options:     *readerOpts,
		Metrics: redisclient.Metrics{
			ReadErrorsInc: func() {
				redisStreamsSourceReadErrors.With(map[string]string{metrics.LabelVertex: vertexSpec.Name, metrics.LabelPipeline: vertexSpec.PipelineName}).Inc()
			},
			ReadsAdd: func(count int) {
				redisStreamsSourceReadCount.With(map[string]string{metrics.LabelVertex: vertexSpec.Name, metrics.LabelPipeline: vertexSpec.PipelineName}).Add(float64(count))
			},
			AcksAdd: func(count int) {
				redisStreamsSourceAckCount.With(map[string]string{metrics.LabelVertex: vertexSpec.Name, metrics.LabelPipeline: vertexSpec.PipelineName}).Add(float64(count))
			},
		},
	}

	redisStreamsSource := &redisStreamsSource{
		RedisStreamsRead: redisStreamsReader,
		pipelineName:     vertexSpec.PipelineName,
	}

	for _, o := range opts {
		operr := o(redisStreamsSource)
		if operr != nil {
			return nil, operr
		}
	}
	if redisStreamsReader.Log == nil {
		redisStreamsReader.Log = logging.NewLogger()
	}

	// Create InterStepDataForward
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

	// Create Watermark Publisher
	ctx, cancel := context.WithCancel(context.Background())
	redisStreamsSource.cancelfn = cancel
	entityName := fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	processorEntity := processor.NewProcessorEntity(entityName)
	redisStreamsSource.sourcePublishWM = publish.NewPublish(ctx, processorEntity, publishWMStores, publish.IsSource(), publish.WithDelay(vertexInstance.Vertex.Spec.Watermark.GetMaxDelay()))

	// create the ConsumerGroup here if not already created
	err = redisStreamsSource.createConsumerGroup(ctx, redisSpec)
	if err != nil {
		return nil, err
	}

	// function which takes the messages that have been read from the Stream and turns them into ReadMessages
	redisStreamsSource.XStreamToMessages = func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
		// for each XMessage in []XStream
		for _, xstream := range xstreams {
			for _, message := range xstream.Messages {
				var readOffset = message.ID

				valIndex := 0
				for f, v := range message.Values {

					// need to make sure the ID is unique: we can make it a combination of the original
					// message ID from Redis (timestamp+sequence num) plus its ordinal in the key/value pairs
					// contained in the message
					id := fmt.Sprintf("%s-%d", message.ID, valIndex)
					valIndex = valIndex + 1

					isbMsg := isb.Message{
						Header: isb.Header{
							MessageInfo: isb.MessageInfo{EventTime: time.Now()}, //doesn't seem like Redis offers a timestamp
							ID:          id,                                     // assumption is that this only needs to be unique for this source vertex
							Key:         f,
						},
						Body: isb.Body{Payload: []byte(v.(string))},
					}

					readMsg := &isb.ReadMessage{
						ReadOffset: isb.SimpleStringOffset(func() string { return readOffset }), // assumption is that this is just used for ack, so doesn't need to include stream name
						Message:    isbMsg,
					}
					messages = append(messages, readMsg)
				}
			}
		}

		return messages, nil
	}

	return redisStreamsSource, nil
}

// create a RedisClient from the RedisStreamsSource spec
func newRedisClient(sourceSpec *dfv1.RedisStreamsSource) (*redisclient.RedisClient, error) {
	password, _ := sharedutil.GetSecretFromVolume(sourceSpec.Password)
	opts := &redis.UniversalOptions{
		Username:     sourceSpec.User,
		Password:     password,
		MasterName:   sourceSpec.MasterName,
		MaxRedirects: 3,
	}
	if opts.MasterName != "" {
		urls := sourceSpec.SentinelURL
		if urls != "" {
			opts.Addrs = strings.Split(urls, ",")
		}
		sentinelPassword, _ := sharedutil.GetSecretFromVolume(sourceSpec.SentinelPassword)
		opts.SentinelPassword = os.Getenv(sentinelPassword)
	} else {
		urls := sourceSpec.URL
		if urls != "" {
			opts.Addrs = strings.Split(urls, ",")
		}
	}

	return redisclient.NewRedisClient(opts), nil
}

func (rsSource *redisStreamsSource) createConsumerGroup(ctx context.Context, sourceSpec *dfv1.RedisStreamsSource) error {
	// user can configure to read stream either from the beginning or from the most recent messages
	readFrom := redisclient.ReadFromLatest
	if sourceSpec.ReadFromBeginning {
		readFrom = redisclient.ReadFromEarliest
	}
	rsSource.Log.Infof("Creating Redis Stream group %q on Stream %q (readFrom=%v)", rsSource.Group, rsSource.Stream, readFrom)
	err := rsSource.RedisClient.CreateStreamGroup(ctx, rsSource.Stream, rsSource.Group, readFrom)
	if err != nil {
		if redisclient.IsAlreadyExistError(err) {
			rsSource.Log.Infow("Consumer Group on Stream already exists.", zap.String("group", rsSource.Group), zap.String("stream", rsSource.Stream))
		} else {
			return fmt.Errorf("failed to create consumer group %q on redis stream %q: err=%v", rsSource.Group, rsSource.Stream, err)
		}
	}
	return nil
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
	rsSource.Log.Info("Shutting down redis source server...")
	rsSource.cancelfn()
	if err := rsSource.sourcePublishWM.Close(); err != nil {
		rsSource.Log.Errorw("Failed to close source vertex watermark publisher", zap.Error(err))
	}
	rsSource.Log.Info("Redis source server shutdown")
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
