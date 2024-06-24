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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
)

type kafkaSource struct {
	vertexName    string              // name of the source vertex
	pipelineName  string              // name of the pipeline
	groupName     string              // group name for the source vertex
	topic         string              // topic to consume messages from
	brokers       []string            // kafka brokers
	lifecycleCtx  context.Context     // lifecycle context
	handler       *ConsumerHandler    // handler for a kafka consumer group
	config        *sarama.Config      // sarama config for kafka consumer group
	logger        *zap.SugaredLogger  // logger
	stopCh        chan struct{}       // channel to indicate that we are done
	handlerBuffer int                 // size of the buffer that holds consumed but yet to be forwarded messages
	readTimeout   time.Duration       // read timeout for the from buffer
	adminClient   sarama.ClusterAdmin // client used to calculate pending messages
	saramaClient  sarama.Client       // sarama client
}

// NewKafkaSource returns a kafkaSource reader based on Kafka Consumer Group.
func NewKafkaSource(ctx context.Context, vertexInstance *dfv1.VertexInstance, handler *ConsumerHandler, opts ...Option) (sourcer.SourceReader, error) {

	source := vertexInstance.Vertex.Spec.Source.Kafka
	ks := &kafkaSource{
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		topic:         source.Topic,
		brokers:       source.Brokers,
		readTimeout:   1 * time.Second, // default timeout
		handlerBuffer: 100,             // default buffer size for kafka reads
		handler:       handler,
		logger:        logging.FromContext(ctx), // default logger
	}

	for _, o := range opts {
		if err := o(ks); err != nil {
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

	sarama.Logger = zap.NewStdLog(ks.logger.Desugar())

	// return errors from the underlying kafka client using the Errors channel
	config.Consumer.Return.Errors = true
	ks.config = config
	ks.lifecycleCtx = ctx

	ks.stopCh = make(chan struct{})

	// create a sarama client and a cluster admin client
	client, err := sarama.NewClient(ks.brokers, ks.config)
	if err != nil {
		ks.logger.Panicw("Failed to create sarama client", zap.Error(err))
	} else {
		ks.saramaClient = client
	}
	// Does it require any special privileges to create a cluster admin client?
	adminClient, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		if !client.Closed() {
			_ = client.Close()
		}
		ks.logger.Panicw("Failed to create sarama cluster admin client", zap.Error(err))
	} else {
		ks.adminClient = adminClient
	}

	go ks.startConsumer()
	// wait for the consumer to setup.
	<-ks.handler.ready
	ks.logger.Info("Consumer ready. Starting kafka reader...")
	return ks, nil
}

// GetName returns the name of the source.
func (ks *kafkaSource) GetName() string {
	return ks.vertexName
}

// Partitions returns the partitions from which the source is reading.
func (ks *kafkaSource) Partitions(context.Context) []int32 {
	for topic, partitions := range ks.handler.sess.Claims() {
		if topic == ks.topic {
			return partitions
		}
	}
	return []int32{}
}

func (ks *kafkaSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	msgs := make([]*isb.ReadMessage, 0, count)
	timeout := time.After(ks.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-ks.handler.messages:
			kafkaSourceReadCount.With(map[string]string{
				metrics.LabelVertex:        ks.vertexName,
				metrics.LabelPipeline:      ks.pipelineName,
				metrics.LabelPartitionName: strconv.Itoa(int(m.Partition)),
			}).Inc()
			msgs = append(msgs, ks.toReadMessage(m))
		case <-timeout:
			// log that timeout has happened and don't return an error
			ks.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", ks.readTimeout))
			break loop
		}
	}

	return msgs, nil
}

// Ack acknowledges an array of offset.
func (ks *kafkaSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	// we want to block the handler from exiting if there are any inflight acks.
	ks.handler.inflightAcks = make(chan bool)
	defer close(ks.handler.inflightAcks)

	for _, offset := range offsets {
		topic := offset.(*kafkaOffset).Topic()

		pOffset, err := offset.Sequence()
		if err != nil {
			kafkaSourceOffsetAckErrors.With(map[string]string{metrics.LabelVertex: ks.vertexName, metrics.LabelPipeline: ks.pipelineName}).Inc()
			ks.logger.Errorw("Unable to extract partition offset of type int64 from the supplied offset. skipping and continuing", zap.String("supplied-offset", offset.String()), zap.Error(err))
			continue
		}
		// we need to mark the offset of the next message to read
		ks.handler.sess.MarkOffset(topic, offset.PartitionIdx(), pOffset+1, "")
		kafkaSourceAckCount.With(map[string]string{metrics.LabelVertex: ks.vertexName, metrics.LabelPipeline: ks.pipelineName}).Inc()

	}
	// How come it does not return errors at all?
	return make([]error, len(offsets))
}

func (ks *kafkaSource) Close() error {
	ks.logger.Info("Closing kafka reader...")
	// finally, shut down the client
	if ks.adminClient != nil {
		// closes the underlying sarama client as well.
		if err := ks.adminClient.Close(); err != nil {
			ks.logger.Errorw("Error in closing kafka admin client", zap.Error(err))
		}
	}
	<-ks.stopCh
	ks.logger.Info("Kafka reader closed")
	return nil
}

func (ks *kafkaSource) Pending(_ context.Context) (int64, error) {
	if ks.adminClient == nil || ks.saramaClient == nil {
		return isb.PendingNotAvailable, nil
	}
	partitions, err := ks.saramaClient.Partitions(ks.topic)
	if err != nil {
		return isb.PendingNotAvailable, fmt.Errorf("failed to get partitions, %w", err)
	}
	totalPending := int64(0)
	rep, err := ks.adminClient.ListConsumerGroupOffsets(ks.groupName, map[string][]int32{ks.topic: partitions})
	if err != nil {
		err := ks.refreshAdminClient()
		if err != nil {
			return isb.PendingNotAvailable, fmt.Errorf("failed to update the admin client, %w", err)
		}
		return isb.PendingNotAvailable, fmt.Errorf("failed to list consumer group offsets, %w", err)
	}
	for _, partition := range partitions {
		block := rep.GetBlock(ks.topic, partition)
		if block.Offset == -1 {
			// Note: if there is no offset associated with the partition under the consumer group, offset fetch sets the offset field to -1.
			// This is not an error and usually means that there has been no data published to this particular partition yet.
			// In this case, we can safely skip this partition from the pending calculation.
			continue
		}
		partitionOffset, err := ks.saramaClient.GetOffset(ks.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return isb.PendingNotAvailable, fmt.Errorf("failed to get offset of topic %q, partition %v, %w", ks.topic, partition, err)
		}
		totalPending += partitionOffset - block.Offset
	}
	kafkaPending.WithLabelValues(ks.vertexName, ks.pipelineName, ks.topic, ks.groupName).Set(float64(totalPending))
	return totalPending, nil
}

// refreshAdminClient refreshes the admin client
func (ks *kafkaSource) refreshAdminClient() error {
	if _, err := ks.saramaClient.RefreshController(); err != nil {
		return fmt.Errorf("failed to refresh controller, %w", err)
	}
	// we are not closing the old admin client because it will close the underlying sarama client as well
	// it is safe to not close the admin client,
	// since we are using the same sarama client, we will not leak any resources(tcp connections)
	admin, err := sarama.NewClusterAdminFromClient(ks.saramaClient)
	if err != nil {
		return fmt.Errorf("failed to create new admin client, %w", err)
	}
	ks.adminClient = admin
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

func (ks *kafkaSource) startConsumer() {
	consumerGroup, err := sarama.NewConsumerGroupFromClient(ks.groupName, ks.saramaClient)
	ks.logger.Infow("creating NewConsumerGroup", zap.String("topic", ks.topic), zap.String("consumerGroupName", ks.groupName), zap.Strings("brokers", ks.brokers))
	if err != nil {
		ks.logger.Panicw("Problem initializing sarama consumerGroup", zap.Error(err))
	}
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ks.lifecycleCtx.Done():
				return
			case cErr := <-consumerGroup.Errors():
				ks.logger.Errorw("Kafka consumer error", zap.Error(cErr))
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
			if conErr := consumerGroup.Consume(ks.lifecycleCtx, []string{ks.topic}, ks.handler); conErr != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}

				// Panic on errors to let it crash and restart the process
				ks.logger.Panicw("Kafka consumer failed with error: ", zap.Error(conErr))
			}

			// check if context was cancelled, signaling that the consumer should stop
			if ks.lifecycleCtx.Err() != nil {
				return
			}
		}
	}()
	wg.Wait()
	_ = consumerGroup.Close()
	close(ks.stopCh)
}

func (ks *kafkaSource) toReadMessage(m *sarama.ConsumerMessage) *isb.ReadMessage {
	readOffset := &kafkaOffset{
		offset:       m.Offset,
		partitionIdx: m.Partition,
		topic:        m.Topic,
	}

	var body = isb.Body{Payload: m.Value}
	var headers = make(map[string]string, len(m.Headers))
	for _, header := range m.Headers {
		headers[string(header.Key)] = string(header.Value)
	}

	msg := isb.Message{
		Header: isb.Header{
			MessageInfo: isb.MessageInfo{EventTime: m.Timestamp},
			ID: isb.MessageID{
				VertexName: ks.vertexName,
				Offset:     readOffset.String(),
				Index:      readOffset.PartitionIdx(),
			},
			Keys:    []string{string(m.Key)},
			Headers: headers,
		},
		Body: body,
	}

	return &isb.ReadMessage{
		ReadOffset: readOffset,
		Message:    msg,
	}
}
