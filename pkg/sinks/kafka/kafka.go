package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/applier"
)

// ToKafka produce the output to a kafka sinks.
type ToKafka struct {
	name         string
	pipelineName string
	producer     sarama.AsyncProducer
	topic        string
	isdf         *forward.InterStepDataForward
	log          *zap.SugaredLogger
	concurrency  uint32
}

type Option func(*ToKafka) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(t *ToKafka) error {
		t.log = log
		return nil
	}
}

// NewToKafka returns ToKafka type.
func NewToKafka(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, opts ...Option) (*ToKafka, error) {
	kafkaSink := vertex.Spec.Sink.Kafka
	toKafka := new(ToKafka)
	toKafka.concurrency = kafkaSink.Concurrency
	//apply options for kafka sink
	for _, o := range opts {
		if err := o(toKafka); err != nil {
			return nil, err
		}
	}

	//set default logger
	if toKafka.log == nil {
		toKafka.log = logging.NewLogger()
	}
	toKafka.log = toKafka.log.With("sinkType", "kafka").With("topic", kafkaSink.Topic)
	toKafka.name = vertex.Spec.Name
	toKafka.pipelineName = vertex.Spec.PipelineName
	toKafka.topic = kafkaSink.Topic

	forwardOpts := []forward.Option{forward.WithLogger(toKafka.log)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	f, err := forward.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{vertex.Name: toKafka}, forward.All, applier.Terminal, nil, forwardOpts...)
	if err != nil {
		return nil, err
	}
	toKafka.isdf = f
	config, err := util.GetSaramaConfigFromYAMLString(kafkaSink.Config)
	if err != nil {
		return nil, err
	}
	if t := kafkaSink.TLS; t != nil {
		config.Net.TLS.Enable = true
		if c, err := util.GetTLSConfig(t); err != nil {
			return nil, err
		} else {
			config.Net.TLS.Config = c
		}
	}
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer(kafkaSink.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer. %w", err)
	}
	toKafka.producer = producer
	return toKafka, nil
}

// GetName returns the name.
func (tk *ToKafka) GetName() string {
	return tk.name
}

// Write writes to the kafka topic.
func (tk *ToKafka) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	errs := make([]error, len(messages))
	for i := 0; i < len(errs); i++ {
		errs[i] = fmt.Errorf("unknown error")
	}
	done := make(chan struct{})
	timeout := time.After(3 * time.Second)
	go func() {
		sent := 0
		for {
			if sent == len(messages) {
				close(done)
				return
			}
			select {
			case err := <-tk.producer.Errors():
				idx := err.Msg.Metadata.(int)
				errs[idx] = err.Err
				sent++
			case m := <-tk.producer.Successes():
				idx := m.Metadata.(int)
				errs[idx] = nil
				sent++
			case <-timeout:
				close(done)
			default:
			}
		}
	}()
	for index, msg := range messages {
		message := &sarama.ProducerMessage{
			Topic:    tk.topic,
			Value:    sarama.ByteEncoder(msg.Payload),
			Metadata: index, // Use metadata to identify if it succeedes or fails in the async return.
		}
		tk.producer.Input() <- message
	}
	<-done
	for _, err := range errs {
		if err != nil {
			kafkaSinkWriteErrors.With(map[string]string{"vertex": tk.name, "pipeline": tk.pipelineName}).Inc()
		} else {
			kafkaSinkWriteCount.With(map[string]string{"vertex": tk.name, "pipeline": tk.pipelineName}).Inc()
		}
	}
	return nil, errs
}

func (tk *ToKafka) Close() error {
	tk.log.Info("Closing kafka producer...")
	return tk.producer.Close()
}

// Start starts sinking to kafka.
func (tk *ToKafka) Start() <-chan struct{} {
	return tk.isdf.Start()
}

// Stop stops sinking
func (tk *ToKafka) Stop() {
	tk.isdf.Stop()
	tk.log.Info("forwarder stopped successfully")
}

// ForceStop stops sinking
func (tk *ToKafka) ForceStop() {
	tk.isdf.ForceStop()
	tk.log.Info("forwarder force stopped successfully")
}
