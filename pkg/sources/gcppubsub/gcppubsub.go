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

package gcppubsub

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
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

type gcpPubSubSource struct {
	name         string
	pipelineName string
	logger       *zap.SugaredLogger

	gcpPubSubClient      *pubsub.Client
	gcpPubSubSubcription *pubsub.Subscription

	bufferSize  int
	messages    chan *isb.ReadMessage
	readTimeout time.Duration

	ctx       context.Context
	cancelFn  context.CancelFunc
	forwarder *sourceforward.DataForward
	// source watermark publisher
	sourcePublishWM publish.Publisher
}

func New(
	ctx context.Context,
	cancelFn context.CancelFunc,
	client *pubsub.Client,
	subscription *pubsub.Subscription,
	vertexInstance *dfv1.VertexInstance,
	writers map[string][]isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	transformerApplier applier.SourceTransformApplier,
	fetchWM fetch.Fetcher,
	toVertexPublisherStores map[string]store.WatermarkStore,
	publishWMStores store.WatermarkStore,
	idleManager wmb.IdleManager,
	opts ...Option) (*gcpPubSubSource, error) {

	pss := &gcpPubSubSource{
		ctx:                  ctx,
		cancelFn:             cancelFn,
		name:                 vertexInstance.Vertex.Spec.Name,
		pipelineName:         vertexInstance.Vertex.Spec.PipelineName,
		bufferSize:           1000,            // default size
		readTimeout:          1 * time.Second, // default timeout
		gcpPubSubClient:      client,
		gcpPubSubSubcription: subscription,
	}
	for _, o := range opts {
		if err := o(pss); err != nil {
			return nil, err
		}
	}
	if pss.logger == nil {
		pss.logger = logging.NewLogger()
	}
	pss.messages = make(chan *isb.ReadMessage, pss.bufferSize)

	forwardOpts := []sourceforward.Option{sourceforward.WithLogger(pss.logger)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sourceforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := sourceforward.NewDataForward(vertexInstance.Vertex, pss, writers, fsd, transformerApplier, fetchWM, pss, toVertexPublisherStores, idleManager, forwardOpts...)
	if err != nil {
		pss.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	pss.forwarder = forwarder
	entityName := fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	processorEntity := entity.NewProcessorEntity(entityName)
	// toVertexPartitionCount is 1 because we publish watermarks within the source itself.
	pss.sourcePublishWM = publish.NewPublish(ctx, processorEntity, publishWMStores, 1, publish.IsSource(), publish.WithDelay(vertexInstance.Vertex.Spec.Watermark.GetMaxDelay()))

	go func() {
		pss.logger.Info("Starting GCPPubSub subscription...")
		err := pss.gcpPubSubSubcription.Receive(pss.ctx, func(msgCtx context.Context, msg *pubsub.Message) {
			readOffset := isb.NewSimpleStringPartitionOffset(msg.ID, 1)
			m := &isb.ReadMessage{
				Message: isb.Message{
					Header: isb.Header{
						MessageInfo: isb.MessageInfo{EventTime: msg.PublishTime},
						ID:          readOffset.String(),
					},
					Body: isb.Body{
						Payload: msg.Data,
					},
				},
				ReadOffset: readOffset,
			}
			pss.messages <- m
		})
		if err != nil {
			pss.logger.Errorf("failed to Receive from GCPPubSub subscription, %w", err)
			pss.cancelFn()
		}
	}()

	return pss, nil
}

func PrepareSubscription(ctx context.Context, vertexInstance *dfv1.VertexInstance, logger *zap.SugaredLogger) (*pubsub.Client, *pubsub.Subscription, error) {
	pubsubEventSource := vertexInstance.Vertex.Spec.Source.GCPPubSub

	opts := make([]option.ClientOption, 0, 1)
	if secret := pubsubEventSource.CredentialSecret; secret != nil {
		logger.Debug("using credentials from secret")
		jsonCred, err := sharedutil.GetSecretFromVolume(secret)
		if err != nil {
			return nil, nil, fmt.Errorf("could not find credentials, %w", err)
		}
		opts = append(opts, option.WithCredentialsJSON([]byte(jsonCred)))
	} else {
		logger.Debug("using default credentials")
	}
	client, err := pubsub.NewClient(ctx, pubsubEventSource.ProjectID, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set up client for GCPPubSub, %w", err)
	}
	logger.Debug("set up pubsub client")

	subscription := client.Subscription(pubsubEventSource.SubscriptionID)

	// Overall logics are as follows:
	//
	// subsc. exists | topic given | topic exists | action                | required permissions
	// :------------ | :---------- | :----------- | :-------------------- | :-----------------------------------------------------------------------------
	// no            | no          | -            | invalid               | -
	// yes           | no          | -            | do nothing            | nothing extra
	// yes           | yes         | -            | verify topic          | pubsub.subscriptions.get (subsc.)
	// no            | yes         | yes          | create subsc.         | pubsub.subscriptions.create (proj.) + pubsub.topics.attachSubscription (topic)
	// no            | yes         | no           | create topic & subsc. | above + pubsub.topics.create (proj. for topic)

	subscExists := false
	if addr := os.Getenv("PUBSUB_EMULATOR_HOST"); addr != "" {
		logger.Debug("using pubsub emulator - skipping permissions check")
		subscExists, err = subscription.Exists(ctx)
		if err != nil {
			client.Close()
			return nil, nil, fmt.Errorf("failed to check if subscription %s exists", subscription)
		}
	} else {
		// trick: you don't need to have get permission to check only whether it exists
		perms, err := subscription.IAM().TestPermissions(ctx, []string{"pubsub.subscriptions.consume"})
		subscExists = len(perms) == 1
		if !subscExists {
			switch status.Code(err) {
			case codes.OK:
				client.Close()
				return nil, nil, fmt.Errorf("you lack permission to pull from %s", subscription)
			case codes.NotFound:
				// OK, maybe the subscription doesn't exist yet, so create it later
				// (it possibly means project itself doesn't exist, but it's ok because we'll see an error later in such case)
			default:
				client.Close()
				return nil, nil, fmt.Errorf("failed to test permission for subscription %s, %w", subscription, err)
			}
		}
		logger.Debug("checked if subscription exists and you have right permission")
	}

	// subsc. exists | topic given | topic exists | action                | required permissions
	// :------------ | :---------- | :----------- | :-------------------- | :-----------------------------------------------------------------------------
	// no            | no          | -            | invalid               | -
	// yes           | no          | -            | do nothing            | nothing extra
	if pubsubEventSource.Topic == "" {
		if !subscExists {
			client.Close()
			return nil, nil, fmt.Errorf("you need to specify topicID to create missing subscription %s", subscription)
		}
		logger.Debug("subscription exists and no topic given, fine")
		return client, subscription, nil
	}

	// subsc. exists | topic given | topic exists | action                | required permissions
	// :------------ | :---------- | :----------- | :-------------------- | :-----------------------------------------------------------------------------
	// yes           | yes         | -            | verify topic          | pubsub.subscriptions.get (subsc.)
	topic := client.TopicInProject(pubsubEventSource.Topic, pubsubEventSource.TopicProjectID)

	if subscExists {
		subscConfig, err := subscription.Config(ctx)
		if err != nil {
			client.Close()
			return nil, nil, fmt.Errorf("failed to get subscription's config for verifying topic, %w", err)
		}
		switch actualTopic := subscConfig.Topic.String(); actualTopic {
		case "_deleted-topic_":
			client.Close()
			return nil, nil, fmt.Errorf("the topic for the subscription has been deleted")
		case topic.String():
			logger.Debug("subscription exists and its topic matches given one, fine")
			return client, subscription, nil
		default:
			client.Close()
			return nil, nil, fmt.Errorf("this subscription belongs to wrong topic %s", actualTopic)
		}
	}

	// subsc. exists | topic given | topic exists | action                | required permissions
	// :------------ | :---------- | :----------- | :-------------------- | :-----------------------------------------------------------------------------
	// no            | yes         | ???          | create subsc.         | pubsub.subscriptions.create (proj.) + pubsub.topics.attachSubscription (topic)
	//                               ↑ We don't know yet, but just try to create subsc.
	logger.Debug("subscription doesn't seem to exist")
	_, err = client.CreateSubscription(ctx, subscription.ID(), pubsub.SubscriptionConfig{Topic: topic})
	switch status.Code(err) {
	case codes.OK:
		logger.Debug("subscription created")
		return client, subscription, nil
	case codes.NotFound:
		// OK, maybe the topic doesn't exist yet, so create it later
		// (it possibly means project itself doesn't exist, but it's ok because we'll see an error later in such case)
	default:
		client.Close()
		return nil, nil, fmt.Errorf("failed to create %s for %s, %w", subscription, topic, err)
	}

	// subsc. exists | topic given | topic exists | action                | required permissions
	// :------------ | :---------- | :----------- | :-------------------- | :-----------------------------------------------------------------------------
	// no            | yes         | no           | create topic & subsc. | above + pubsub.topics.create (proj. for topic)
	logger.Debug("topic doesn't seem to exist neither")
	// NB: you need another client for topic because it might be in different project
	topicClient, err := pubsub.NewClient(ctx, pubsubEventSource.TopicProjectID, opts...)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("failed to create client to create %s, %w", topic, err)
	}
	defer topicClient.Close()

	_, err = topicClient.CreateTopic(ctx, topic.ID())
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("failed to create %s, %w", topic, err)
	}
	logger.Debug("topic created")
	_, err = client.CreateSubscription(ctx, subscription.ID(), pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("failed to create %s for %s, %w", subscription, topic, err)
	}
	logger.Debug("subscription created")
	return client, subscription, nil
}

type Option func(*gcpPubSubSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *gcpPubSubSource) error {
		o.logger = l
		return nil
	}
}

// WithBufferSize sets the buffer size for storing the messages from GCPPubSub
func WithBufferSize(s int) Option {
	return func(o *gcpPubSubSource) error {
		o.bufferSize = s
		return nil
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(t time.Duration) Option {
	return func(o *gcpPubSubSource) error {
		o.readTimeout = t
		return nil
	}
}

func (pss *gcpPubSubSource) GetName() string {
	return pss.name
}

// GetPartitionIdx returns the partition number for the source vertex buffer
// Source is like a buffer with only one partition. So, we always return 0
func (pss *gcpPubSubSource) GetPartitionIdx() int32 {
	return 0
}

func (pss *gcpPubSubSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	var msgs []*isb.ReadMessage
	timeout := time.After(pss.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-pss.messages:
			gcpPubSubSourceReadCount.With(map[string]string{metrics.LabelVertex: pss.name, metrics.LabelPipeline: pss.pipelineName}).Inc()
			msgs = append(msgs, m)
		case <-timeout:
			pss.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", pss.readTimeout), zap.Int("read", len(msgs)))
			break loop
		}
	}
	pss.logger.Debugw("Read %d messages.", len(msgs))
	return msgs, nil
}

func (pss *gcpPubSubSource) Pending(_ context.Context) (int64, error) {
	return isb.PendingNotAvailable, nil
}

func (pss *gcpPubSubSource) PublishSourceWatermarks(msgs []*isb.ReadMessage) {
	var oldest time.Time
	for _, m := range msgs {
		if oldest.IsZero() || m.EventTime.Before(oldest) {
			oldest = m.EventTime
		}
	}
	if len(msgs) > 0 && !oldest.IsZero() {
		// toVertexPartitionIdx is 0 because we publish watermarks within the source itself.
		pss.sourcePublishWM.PublishWatermark(wmb.Watermark(oldest), nil, 0) // Source publisher does not care about the offset
	}
}

func (pss *gcpPubSubSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	return make([]error, len(offsets))
}

func (pss *gcpPubSubSource) NoAck(_ context.Context, _ []isb.Offset) {}

func (pss *gcpPubSubSource) Close() error {
	pss.logger.Info("closing PubSub client...")
	if err := pss.gcpPubSubClient.Close(); err != nil {
		pss.logger.Errorw("failed to close the PubSub client", zap.Error(err))
	}
	pss.cancelFn()
	return nil
}

func (pss *gcpPubSubSource) Stop() {
	pss.forwarder.Stop()
}

func (pss *gcpPubSubSource) ForceStop() {
	pss.Stop()
}

func (pss *gcpPubSubSource) Start() <-chan struct{} {
	return pss.forwarder.Start()
}
