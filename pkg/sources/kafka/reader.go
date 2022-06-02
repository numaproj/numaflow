package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/applier"
)

type KafkaSource struct {
	// name of the source vertex
	name string
	// name of the pipeline
	pipelineName string
	// group name for the source vertex
	groupname string
	// topic to consume messages from
	topic string
	// kafka brokers
	brokers []string
	// forwarder that writes the consumed data to destination
	forwarder *forward.InterStepDataForward
	// context cancel function
	cancelfn context.CancelFunc
	// lifecycle context
	lifecyclectx context.Context
	// consumergroup handler for kafka consumer group
	handler *consumerHandler
	// sarama config for kafka consumer group
	config *sarama.Config
	// logger
	logger *zap.SugaredLogger
	// channel to inidcate that we are done
	stopch chan struct{}
	// size of the buffer that holds consumed but yet to be forwarded messages
	handlerbuffer int
	// read timeout for the from buffer
	readTimeout time.Duration
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
		o.handlerbuffer = s
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
		o.groupname = gn
		return nil
	}
}

func (r *KafkaSource) GetName() string {
	return r.name
}

// Read reads a chunk of messages and returns at the first occurrence of an error. Error does not indicate that the
// array of result is empty, the callee should process all the elements in the array even if the error is set. Read
// will not mark the message in the buffer as "READ" if the read for that index is erring.
// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages.
func (r *KafkaSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	msgs := make([]*isb.ReadMessage, 0, count)
	var i int64
	for i = 0; i < count; i++ {
		select {
		case m := <-r.handler.messages:
			kafkaSourceReadCount.With(map[string]string{"vertex": r.name, "pipeline": r.pipelineName}).Inc()
			msgs = append(msgs, toReadMessage(m))

		case <-time.After(r.readTimeout):
			// log that timeout has happened and don't return an error
			r.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", r.readTimeout))
			return msgs, nil
		}
	}

	return msgs, nil
}

// Ack acknowledges an array of offset.
func (r *KafkaSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	// we want to block the handler from exiting if there are any inflight acks.
	r.handler.inflightacks = make(chan bool)
	defer close(r.handler.inflightacks)

	for _, offset := range offsets {
		topic, partition, poffset, err := offsetFrom(offset.String())
		if err != nil {
			kafkaSourceOffsetAckErrors.With(map[string]string{"vertex": r.name, "pipeline": r.pipelineName}).Inc()
			r.logger.Errorw("Unable to extract partition offset of type int64 from the supplied offset. skipping and continuing", zap.String("suppliedoffset", offset.String()), zap.Error(err))
			continue
		}

		// we need to mark the offset of the next message to read
		r.handler.sess.MarkOffset(topic, partition, poffset+1, "")
		kafkaSourceAckCount.With(map[string]string{"vertex": r.name, "pipeline": r.pipelineName}).Inc()

	}
	// How come it does not return errors at all?
	return make([]error, len(offsets))
}

func (r *KafkaSource) Start() <-chan struct{} {
	go r.startConsumer()
	// wait for the consumer to setup.
	<-r.handler.ready

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
	r.cancelfn()
	<-r.stopch
	r.logger.Info("Kafka reader closed")
	return nil
}

// NewKafkaSource returns a KafkaSource reader based on Kafka Consumer Group .
func NewKafkaSource(vertex *dfv1.Vertex, writers []isb.BufferWriter, opts ...Option) (*KafkaSource, error) {
	source := vertex.Spec.Source.Kafka
	kafkasource := &KafkaSource{
		name:          vertex.Spec.Name,
		pipelineName:  vertex.Spec.PipelineName,
		topic:         source.Topic,
		brokers:       source.Brokers,
		readTimeout:   1 * time.Second, // default timeout
		handlerbuffer: 100,             // default buffer size for kafka reads
	}

	for _, o := range opts {
		operr := o(kafkasource)
		if operr != nil {
			return nil, operr
		}
	}

	config, err := configFromOpts(source.Config)
	if err != nil {
		return nil, fmt.Errorf("error reading kafka source config, %w", err)
	}

	if t := source.TLS; t != nil {
		config.Net.TLS.Enable = true
		if c, err := util.GetTLSConfig(t); err != nil {
			return nil, err
		} else {
			config.Net.TLS.Config = c
		}
	}

	kafkasource.config = config

	ctx, cancel := context.WithCancel(context.Background())
	kafkasource.cancelfn = cancel
	kafkasource.lifecyclectx = ctx

	kafkasource.stopch = make(chan struct{})

	handler := newConsumerHandler(kafkasource.handlerbuffer)
	kafkasource.handler = handler

	destinations := make(map[string]isb.BufferWriter, len(writers))
	for _, w := range writers {
		destinations[w.GetName()] = w
	}

	forwardOpts := []forward.Option{forward.WithLogger(kafkasource.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := forward.NewInterStepDataForward(vertex, kafkasource, destinations, forward.All, applier.Terminal, nil, forwardOpts...)
	if err != nil {
		kafkasource.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	kafkasource.forwarder = forwarder

	return kafkasource, nil
}

func configFromOpts(yamlconfig string) (*sarama.Config, error) {

	config, err := util.GetSaramaConfigFromYAMLString(yamlconfig)
	if err != nil {
		return nil, err
	}
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	return config, nil
}

func (r *KafkaSource) startConsumer() {
	client, err := sarama.NewConsumerGroup(r.brokers, r.groupname, r.config)
	r.logger.Infow("creating NewConsumerGroup", zap.String("topic", r.topic), zap.String("consumerGroupName", r.groupname), zap.Strings("brokers", r.brokers))
	if err != nil {
		r.logger.Panicw("Problem initializing sarama client", zap.Error(err))
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(r.lifecyclectx, []string{r.topic}, r.handler); err != nil {
				r.logger.Warnw("Initialization of consumer failed with error: ", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if r.lifecyclectx.Err() != nil {
				return
			}
		}
	}()
	wg.Wait()
	_ = client.Close()
	close(r.stopch)
}

func toReadMessage(m *sarama.ConsumerMessage) *isb.ReadMessage {

	offset := toOffset(m.Topic, m.Partition, m.Offset)
	msg := isb.Message{
		Header: isb.Header{
			PaneInfo: isb.PaneInfo{EventTime: m.Timestamp},
			ID:       offset,
			Key:      m.Key,
		},
		Body: isb.Body{Payload: m.Value},
	}

	return &isb.ReadMessage{
		ReadOffset: isb.SimpleOffset(func() string { return offset }),
		Message:    msg,
	}
}

func toOffset(topic string, partition int32, offset int64) string {
	// TODO handle this elegantly
	return fmt.Sprintf("%s:%v:%v", topic, partition, offset)
}

func offsetFrom(offset string) (string, int32, int64, error) {
	tokens := strings.Split(offset, ":")
	if len(tokens) < 3 {
		return "", 0, 0, errors.New("malformed offset")
	}
	var poffset, partition int64
	var err error
	poffset, err = strconv.ParseInt(tokens[2], 10, 64)
	if err == nil {
		partition, err = strconv.ParseInt(tokens[1], 10, 32)
	}
	return tokens[0], int32(partition), poffset, err
}
