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

	// create RedisClient and create RedisStreamsReader passing that in
	vertexSpec := vertexInstance.Vertex.Spec
	redisSpec := vertexSpec.Source.RedisStreams
	redisClient, err := newRedisClient(redisSpec)
	if err != nil {
		return nil, err
	}
	fmt.Printf("deletethis: redisSpec=%+v, redisClient=%+v\n", redisSpec, redisClient)

	readerOpts := &redisclient.Options{
		InfoRefreshInterval: time.Second,
		ReadTimeOut:         time.Second,
		CheckBackLog:        true,
	}

	redisStreamsReader := &redisclient.RedisStreamsReader{
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

			//Reads:
			//Acks:
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
	readFrom := redisclient.ReadFromLatest
	if redisSpec.ReadFromBeginning {
		readFrom = redisclient.ReadFromEarliest
	}
	redisStreamsSource.Log.Infof("Creating Redis Stream group %q on Stream %q", redisStreamsReader.Group, redisStreamsReader.Stream)
	err = redisClient.CreateStreamGroup(ctx, redisStreamsReader.Stream, redisStreamsReader.Group, readFrom)
	fmt.Printf("deletethis: error from CreateStreamGroup: %v\n", err)
	// TODO: if err != alreadyCreated { return err }

	redisStreamsSource.XStreamToMessages = func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
		// for each XMessage in []XStream
		for _, xstream := range xstreams {
			for _, message := range xstream.Messages {
				var readOffset = message.ID

				for f, v := range message.Values {

					isbMsg := isb.Message{
						Header: isb.Header{
							MessageInfo: isb.MessageInfo{EventTime: time.Now()}, //doesn't seem like Redis offers a timestamp
							ID:          readOffset,                             // assumption is that this only needs to be unique for this source vertex
							Key:         f,
						},
						Body: isb.Body{Payload: []byte(v.(string))},
					}

					readMsg := &isb.ReadMessage{
						ReadOffset: isb.SimpleStringOffset(func() string { return readOffset }), // assumption is that this is just used for ack, so doesn't need to include stream name
						Message:    isbMsg,
					}
					messages = append(messages, readMsg)
					fmt.Printf("deletethis: added message to output: %+v\n", readMsg)
				}
			}
		}

		return messages, nil
	}

	return redisStreamsSource, nil
}

/*
func toOffset(stream string, offset string) string {
	return fmt.Sprintf("%s:%s", stream, offset)
}*/

func newRedisClient(sourceSpec *dfv1.RedisStreamsSource) (*redisclient.RedisClient, error) {
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
	var err error
	if sourceSpec.Auth != nil && sourceSpec.Auth.User != nil && sourceSpec.Auth.Password != nil {
		opts.Username, err = sharedutil.GetSecretFromVolume(sourceSpec.Auth.User)
		if err != nil {
			return nil, fmt.Errorf("failed to get basic auth user, %w", err)
		}
		opts.Password, err = sharedutil.GetSecretFromVolume(sourceSpec.Auth.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to get basic auth password, %w", err)
		}
	}
	if sourceSpec.TLS != nil {
		if c, err := sharedutil.GetTLSConfig(sourceSpec.TLS); err != nil {
			return nil, err
		} else {
			opts.TLSConfig = c
		}

	}
	return redisclient.NewRedisClient(opts), nil
}

func (rsSource *redisStreamsSource) PublishSourceWatermarks(msgs []*isb.ReadMessage) {
	var oldest time.Time
	for _, m := range msgs {
		if oldest.IsZero() || m.EventTime.Before(oldest) {
			oldest = m.EventTime
		}
	}
	if len(msgs) > 0 && !oldest.IsZero() {
		fmt.Printf("deletethis: publishing watermark for oldest=%v\n", oldest)
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
