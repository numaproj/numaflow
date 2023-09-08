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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	redis2 "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/metrics"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	sourceforward "github.com/numaproj/numaflow/pkg/sources/forward"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type redisStreamsSource struct {
	// shared code to read from Redis Streams
	*redisclient.RedisStreamsRead
	// name of the pipeline
	pipelineName string
	// forwarder that writes the consumed data to destination
	forwarder *sourceforward.DataForward
	// context cancel function
	cancelFn context.CancelFunc
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
	writers map[string][]isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	transformerApplier applier.SourceTransformApplier,
	fetchWM fetch.Fetcher,
	toVertexPublisherStores map[string]store.WatermarkStore,
	publishWMStores store.WatermarkStore,
	idleManager wmb.IdleManagement,
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
			AckErrorsAdd: func(count int) {
				redisStreamsSourceAckErrors.With(map[string]string{metrics.LabelVertex: vertexSpec.Name, metrics.LabelPipeline: vertexSpec.PipelineName}).Add(float64(count))
			},
		},
	}

	redisStreamsSource := &redisStreamsSource{
		RedisStreamsRead: redisStreamsReader,
		pipelineName:     vertexSpec.PipelineName,
	}

	for _, o := range opts {
		if err := o(redisStreamsSource); err != nil {
			return nil, err
		}
	}
	if redisStreamsReader.Log == nil {
		redisStreamsReader.Log = logging.NewLogger()
	}

	// Create Source Data Forwarder
	forwardOpts := []sourceforward.Option{sourceforward.WithLogger(redisStreamsSource.Log)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sourceforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := sourceforward.NewDataForward(vertexInstance.Vertex, redisStreamsSource, writers, fsd, transformerApplier, fetchWM, redisStreamsSource, toVertexPublisherStores, idleManager, forwardOpts...)
	if err != nil {
		redisStreamsSource.Log.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	redisStreamsSource.forwarder = forwarder

	// Create Watermark Publisher
	ctx, cancel := context.WithCancel(context.Background())
	redisStreamsSource.cancelFn = cancel
	entityName := fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	processorEntity := entity.NewProcessorEntity(entityName)
	// toVertexPartitionCount is 1 because we publish watermarks within the source itself.
	redisStreamsSource.sourcePublishWM = publish.NewPublish(ctx, processorEntity, publishWMStores, 1, publish.IsSource(), publish.WithDelay(vertexInstance.Vertex.Spec.Watermark.GetMaxDelay()))

	// create the ConsumerGroup here if not already created
	err = redisStreamsSource.createConsumerGroup(ctx, redisSpec)
	if err != nil {
		return nil, err
	}

	// function which takes the messages that have been read from the Stream and turns them into ReadMessages
	redisStreamsSource.XStreamToMessages = func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
		if len(xstreams) > 1 {
			redisStreamsSource.Log.Warnf("redisStreamsSource shouldn't have more than one Stream; xstreams=%+v", xstreams)
		} else if len(xstreams) == 1 {
			xstream := xstreams[0]

			for _, message := range xstream.Messages {
				var outMsg *isb.ReadMessage
				var err error
				if len(message.Values) >= 1 {
					outMsg, err = produceMsg(message, vertexInstance.Replica)
					if err != nil {
						return nil, err
					}
				} else {
					// don't think there should be a message with no values, but if there is...
					redisStreamsSource.Log.Warnf("unexpected: RedisStreams message has no values? message=%+v", message)
					continue
				}
				messages = append(messages, outMsg)
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

func produceMsg(inMsg redis.XMessage, replica int32) (*isb.ReadMessage, error) {
	var readOffset = redis2.NewRedisOffset(inMsg.ID, replica)

	jsonSerialized, err := json.Marshal(inMsg.Values)
	if err != nil {
		return nil, fmt.Errorf("failed to json serialize RedisStream values: %v; inMsg=%+v", err, inMsg)
	}
	var keys []string
	for k := range inMsg.Values {
		keys = append(keys, k)
	}

	msgTime, err := msgIdToTime(inMsg.ID)
	if err != nil {
		return nil, err
	}
	isbMsg := isb.Message{
		Header: isb.Header{
			MessageInfo: isb.MessageInfo{EventTime: msgTime},
			ID:          inMsg.ID,
			Keys:        keys,
		},
		Body: isb.Body{Payload: jsonSerialized},
	}

	return &isb.ReadMessage{
		ReadOffset: readOffset,
		Message:    isbMsg,
	}, nil
}

// the ID of the message is formatted <msecTime>-<index>
func msgIdToTime(id string) (time.Time, error) {
	splitStr := strings.Split(id, "-")
	if len(splitStr) != 2 {
		return time.Time{}, fmt.Errorf("unexpected message ID value for Redis Streams: %s", id)
	}
	timeMsec, err := strconv.ParseInt(splitStr[0], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	mSecRemainder := timeMsec % 1000
	t := time.Unix(timeMsec/1000, mSecRemainder*1000000)

	return t, nil

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
		// toVertexPartitionIdx is 0 because we publish watermarks within the source itself.
		rsSource.sourcePublishWM.PublishWatermark(wmb.Watermark(oldest), nil, 0) // Source publisher does not care about the offset
	}
}

func (rsSource *redisStreamsSource) Close() error {
	rsSource.Log.Info("Shutting down redis source server...")
	rsSource.cancelFn()
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
