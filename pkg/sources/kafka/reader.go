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

package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

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

type KafkaSource struct {
	// name of the source vertex
	vertexName string
	// name of the pipeline
	pipelineName string
	// group name for the source vertex
	groupName string
	// topic to consume messages from
	topic string
	// kafka brokers
	brokers []string
	// forwarder that writes the consumed data to destination
	forwarder *sourceforward.DataForward
	// context cancel function
	cancelFn context.CancelFunc
	// lifecycle context
	lifecycleCtx context.Context
	// handler for a kafka consumer group
	handler *consumerHandler
	// sarama config for kafka consumer group
	config *sarama.Config
	// logger
	logger *zap.SugaredLogger
	// channel to indicate that we are done
	stopCh chan struct{}
	// size of the buffer that holds consumed but yet to be forwarded messages
	handlerBuffer int
	// read timeout for the from buffer
	readTimeout time.Duration
	// client used to calculate pending messages
	adminClient sarama.ClusterAdmin
	// sarama client
	saramaClient sarama.Client
	// source watermark publishers for different partitions
	sourcePublishWMs map[int32]publish.Publisher
	// max delay duration of watermark
	watermarkMaxDelay time.Duration
	// source watermark publisher stores
	srcPublishWMStores store.WatermarkStore
	lock               *sync.RWMutex
}

// kafkaOffset implements isb.Offset
// we need topic information to ack the message
type kafkaOffset struct {
	offset       int64
	partitionIdx int32
	topic        string
}

func (k *kafkaOffset) String() string {
	return fmt.Sprintf("%s:%d:%d", k.topic, k.offset, k.partitionIdx)
}

func (k *kafkaOffset) Sequence() (int64, error) {
	return k.offset, nil
}

// AckIt acking is taken care by the consumer group
func (k *kafkaOffset) AckIt() error {
	// NOOP
	return nil
}

func (k *kafkaOffset) NoAck() error {
	return nil
}

func (k *kafkaOffset) PartitionIdx() int32 {
	return k.partitionIdx
}

func (k *kafkaOffset) Topic() string {
	return k.topic
}

type Option func(*KafkaSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *KafkaSource) error {
		o.logger = l
		return nil
	}
}

// WithBufferSize is used to return size of message channel information
func WithBufferSize(s int) Option {
	return func(o *KafkaSource) error {
		o.handlerBuffer = s
		return nil
	}
}

// WithReadTimeOut is used to set the read timeout for the from buffer
func WithReadTimeOut(t time.Duration) Option {
	return func(o *KafkaSource) error {
		o.readTimeout = t
		return nil
	}
}

// WithGroupName is used to set the group name
func WithGroupName(gn string) Option {
	return func(o *KafkaSource) error {
		o.groupName = gn
		return nil
	}
}

func (r *KafkaSource) GetName() string {
	return r.vertexName
}

// GetPartitionIdx returns the partition number for the source vertex buffer
// Source is like a buffer with only one partition. So, we always return 0
func (r *KafkaSource) GetPartitionIdx() int32 {
	return 0
}

func (r *KafkaSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	msgs := make([]*isb.ReadMessage, 0, count)
	timeout := time.After(r.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-r.handler.messages:
			kafkaSourceReadCount.With(map[string]string{metrics.LabelVertex: r.vertexName, metrics.LabelPipeline: r.pipelineName}).Inc()
			msgs = append(msgs, toReadMessage(m))
		case <-timeout:
			// log that timeout has happened and don't return an error
			r.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", r.readTimeout))
			break loop
		}
	}
	return msgs, nil
}

func (r *KafkaSource) PublishSourceWatermarks(msgs []*isb.ReadMessage) {
	// oldestTimestamps stores the latest timestamps for different partitions
	oldestTimestamps := make(map[int32]time.Time)
	for _, m := range msgs {
		// Get latest timestamps for different partitions
		if t, ok := oldestTimestamps[m.ReadOffset.PartitionIdx()]; !ok || m.EventTime.Before(t) {
			oldestTimestamps[m.ReadOffset.PartitionIdx()] = m.EventTime
		}
	}
	for p, t := range oldestTimestamps {
		publisher := r.loadSourceWatermarkPublisher(p)
		// toVertexPartitionIdx is 0 because we publish watermarks within the source itself.
		publisher.PublishWatermark(wmb.Watermark(t), nil, 0) // Source publisher does not care about the offset
	}
}

// loadSourceWatermarkPublisher does a lazy load on the watermark publisher
func (r *KafkaSource) loadSourceWatermarkPublisher(partitionID int32) publish.Publisher {
	r.lock.Lock()
	defer r.lock.Unlock()
	if p, ok := r.sourcePublishWMs[partitionID]; ok {
		return p
	}
	entityName := fmt.Sprintf("%s-%s-%d", r.pipelineName, r.vertexName, partitionID)
	processorEntity := entity.NewProcessorEntity(entityName)
	// toVertexPartitionCount is 1 because we publish watermarks within the source itself.
	sourcePublishWM := publish.NewPublish(r.lifecycleCtx, processorEntity, r.srcPublishWMStores, 1, publish.IsSource(), publish.WithDelay(r.watermarkMaxDelay))
	r.sourcePublishWMs[partitionID] = sourcePublishWM
	return sourcePublishWM
}

// Ack acknowledges an array of offset.
func (r *KafkaSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	// we want to block the handler from exiting if there are any inflight acks.
	r.handler.inflightacks = make(chan bool)
	defer close(r.handler.inflightacks)

	for _, offset := range offsets {
		topic := offset.(*kafkaOffset).Topic()

		// we need to mark the offset of the next message to read
		pOffset, err := offset.Sequence()
		if err != nil {
			kafkaSourceOffsetAckErrors.With(map[string]string{metrics.LabelVertex: r.vertexName, metrics.LabelPipeline: r.pipelineName}).Inc()
			r.logger.Errorw("Unable to extract partition offset of type int64 from the supplied offset. skipping and continuing", zap.String("supplied-offset", offset.String()), zap.Error(err))
			continue
		}
		r.handler.sess.MarkOffset(topic, offset.PartitionIdx(), pOffset, "")
		kafkaSourceAckCount.With(map[string]string{metrics.LabelVertex: r.vertexName, metrics.LabelPipeline: r.pipelineName}).Inc()

	}
	// How come it does not return errors at all?
	return make([]error, len(offsets))
}

func (r *KafkaSource) NoAck(_ context.Context, _ []isb.Offset) {}

func (r *KafkaSource) Start() <-chan struct{} {
	client, err := sarama.NewClient(r.brokers, r.config)
	if err != nil {
		r.logger.Panicw("Failed to create sarama client", zap.Error(err))
	} else {
		r.saramaClient = client
	}
	// Does it require any special privileges to create a cluster admin client?
	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		if !client.Closed() {
			_ = client.Close()
		}
		r.logger.Panicw("Failed to create sarama cluster admin client", zap.Error(err))
	} else {
		r.adminClient = adminClient
	}

	go r.startConsumer()
	// wait for the consumer to setup.
	<-r.handler.ready
	r.logger.Info("Consumer ready. Starting kafka reader...")

	return r.forwarder.Start()
}

func (r *KafkaSource) Stop() {
	r.logger.Info("Stopping kafka reader...")
	// stop the forwarder
	r.forwarder.Stop()
}

func (r *KafkaSource) ForceStop() {
	// ForceStop means everything has to stop.
	// don't wait for anything.
	r.forwarder.ForceStop()
	r.Stop()
}

func (r *KafkaSource) Close() error {
	r.logger.Info("Closing kafka reader...")
	// finally, shut down the client
	r.cancelFn()
	if r.adminClient != nil {
		// closes the underlying sarama client as well.
		if err := r.adminClient.Close(); err != nil {
			r.logger.Errorw("Error in closing kafka admin client", zap.Error(err))
		}
	}
	<-r.stopCh
	r.logger.Info("Kafka reader closed")
	return nil
}

func (r *KafkaSource) Pending(_ context.Context) (int64, error) {
	if r.adminClient == nil || r.saramaClient == nil {
		return isb.PendingNotAvailable, nil
	}
	partitions, err := r.saramaClient.Partitions(r.topic)
	if err != nil {
		return isb.PendingNotAvailable, fmt.Errorf("failed to get partitions, %w", err)
	}
	totalPending := int64(0)
	rep, err := r.adminClient.ListConsumerGroupOffsets(r.groupName, map[string][]int32{r.topic: partitions})
	if err != nil {
		err := r.refreshAdminClient()
		if err != nil {
			return isb.PendingNotAvailable, fmt.Errorf("failed to update the admin client, %w", err)
		}
		return isb.PendingNotAvailable, fmt.Errorf("failed to list consumer group offsets, %w", err)
	}
	for _, partition := range partitions {
		block := rep.GetBlock(r.topic, partition)
		if block.Offset == -1 {
			// Note: if there is no offset associated with the partition under the consumer group, offset fetch sets the offset field to -1.
			// This is not an error and usually means that there has been no data published to this particular partition yet.
			// In this case, we can safely skip this partition from the pending calculation.
			continue
		}
		partitionOffset, err := r.saramaClient.GetOffset(r.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return isb.PendingNotAvailable, fmt.Errorf("failed to get offset of topic %q, partition %v, %w", r.topic, partition, err)
		}
		totalPending += partitionOffset - block.Offset
	}
	kafkaPending.WithLabelValues(r.vertexName, r.pipelineName, r.topic, r.groupName).Set(float64(totalPending))
	return totalPending, nil
}

// NewKafkaSource returns a KafkaSource reader based on Kafka Consumer Group.
func NewKafkaSource(
	vertexInstance *dfv1.VertexInstance,
	writers map[string][]isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	transformerApplier applier.SourceTransformApplier,
	fetchWM fetch.Fetcher,
	toVertexPublisherStores map[string]store.WatermarkStore,
	publishWMStores store.WatermarkStore,
	opts ...Option) (*KafkaSource, error) {

	source := vertexInstance.Vertex.Spec.Source.Kafka
	kafkaSource := &KafkaSource{
		vertexName:         vertexInstance.Vertex.Spec.Name,
		pipelineName:       vertexInstance.Vertex.Spec.PipelineName,
		topic:              source.Topic,
		brokers:            source.Brokers,
		readTimeout:        1 * time.Second, // default timeout
		handlerBuffer:      100,             // default buffer size for kafka reads
		srcPublishWMStores: publishWMStores,
		sourcePublishWMs:   make(map[int32]publish.Publisher),
		watermarkMaxDelay:  vertexInstance.Vertex.Spec.Watermark.GetMaxDelay(),
		lock:               new(sync.RWMutex),
		logger:             logging.NewLogger(), // default logger
	}

	for _, o := range opts {
		if err := o(kafkaSource); err != nil {
			return nil, err
		}
	}

	sarama.NewConfig()

	config, err := configFromOpts(source.Config)
	if err != nil {
		return nil, fmt.Errorf("error reading kafka source config, %w", err)
	}

	if t := source.TLS; t != nil {
		config.Net.TLS.Enable = true
		if c, err := sharedutil.GetTLSConfig(t); err != nil {
			return nil, err
		} else {
			config.Net.TLS.Config = c
		}
	}
	if s := source.SASL; s != nil {
		if sasl, err := sharedutil.GetSASL(s); err != nil {
			return nil, err
		} else {
			config.Net.SASL = *sasl
		}
	}

	sarama.Logger = zap.NewStdLog(kafkaSource.logger.Desugar())

	// return errors from the underlying kafka client using the Errors channel
	config.Consumer.Return.Errors = true
	kafkaSource.config = config

	ctx, cancel := context.WithCancel(context.Background())
	kafkaSource.cancelFn = cancel
	kafkaSource.lifecycleCtx = ctx

	kafkaSource.stopCh = make(chan struct{})

	handler := newConsumerHandler(kafkaSource.handlerBuffer)
	kafkaSource.handler = handler

	forwardOpts := []sourceforward.Option{sourceforward.WithLogger(kafkaSource.logger)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sourceforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := sourceforward.NewDataForward(vertexInstance.Vertex, kafkaSource, writers, fsd, transformerApplier, fetchWM, kafkaSource, toVertexPublisherStores, forwardOpts...)
	if err != nil {
		kafkaSource.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	kafkaSource.forwarder = forwarder
	return kafkaSource, nil
}

// refreshAdminClient refreshes the admin client
func (r *KafkaSource) refreshAdminClient() error {
	if _, err := r.saramaClient.RefreshController(); err != nil {
		return fmt.Errorf("failed to refresh controller, %w", err)
	}
	// we are not closing the old admin client because it will close the underlying sarama client as well
	// it is safe to not close the admin client,
	// since we are using the same sarama client, we will not leak any resources(tcp connections)
	admin, err := sarama.NewClusterAdminFromClient(r.saramaClient)
	if err != nil {
		return fmt.Errorf("failed to create new admin client, %w", err)
	}
	r.adminClient = admin
	return nil
}

func configFromOpts(yamlConfig string) (*sarama.Config, error) {
	config, err := sharedutil.GetSaramaConfigFromYAMLString(yamlConfig)
	if err != nil {
		return nil, err
	}
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	return config, nil
}

func (r *KafkaSource) startConsumer() {
	client, err := sarama.NewConsumerGroup(r.brokers, r.groupName, r.config)
	r.logger.Infow("creating NewConsumerGroup", zap.String("topic", r.topic), zap.String("consumerGroupName", r.groupName), zap.Strings("brokers", r.brokers))
	if err != nil {
		r.logger.Panicw("Problem initializing sarama client", zap.Error(err))
	}
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-r.lifecycleCtx.Done():
				return
			case cErr := <-client.Errors():
				r.logger.Errorw("Kafka consumer error", zap.Error(cErr))
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop; when a
			// server-side re-balance happens, the consumer session will need to be
			// recreated to get the new claims
			if conErr := client.Consume(r.lifecycleCtx, []string{r.topic}, r.handler); conErr != nil {
				// Panic on errors to let it crash and restart the process
				r.logger.Panicw("Kafka consumer failed with error: ", zap.Error(conErr))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if r.lifecycleCtx.Err() != nil {
				return
			}
		}
	}()
	wg.Wait()
	_ = client.Close()
	close(r.stopCh)
}

func toReadMessage(m *sarama.ConsumerMessage) *isb.ReadMessage {
	readOffset := &kafkaOffset{
		offset:       m.Offset,
		partitionIdx: m.Partition,
		topic:        m.Topic,
	}
	msg := isb.Message{
		Header: isb.Header{
			MessageInfo: isb.MessageInfo{EventTime: m.Timestamp},
			ID:          readOffset.String(),
			Keys:        []string{string(m.Key)},
		},
		Body: isb.Body{Payload: m.Value},
	}

	return &isb.ReadMessage{
		ReadOffset: readOffset,
		Message:    msg,
	}
}
