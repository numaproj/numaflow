package kafka

import (
	"context"
	"fmt"
	"sync"

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
	producer     sarama.SyncProducer
	topic        string
	isdf         *forward.InterStepDataForward
	log          *zap.SugaredLogger
	concurrency  uint32
}

type Option func(*ToKafka) error

type sinkMessage struct {
	index   int
	message *sarama.ProducerMessage
}

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
	f, err := forward.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{vertex.Name: toKafka}, forward.All, applier.Terminal, forwardOpts...)
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
	//Todo AsyncProducer will be implemented later, with common interface
	producer, err := sarama.NewSyncProducer(kafkaSink.Brokers, config)
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
	wg := new(sync.WaitGroup)

	sinkCh := make(chan *sinkMessage)

	for i := uint32(0); i < tk.concurrency; i++ {
		wg.Add(1)
		go func(msgCh chan *sinkMessage) {
			defer wg.Done()
			for message := range msgCh {
				_, _, err := tk.producer.SendMessage(message.message)
				if err != nil {
					kafkaSinkWriteErrors.With(map[string]string{"vertex": tk.name, "pipeline": tk.pipelineName}).Inc()
					tk.log.Errorw("SendMessage failed", zap.Error(err), zap.Any("message", message))
				} else {
					kafkaSinkWriteCount.With(map[string]string{"vertex": tk.name, "pipeline": tk.pipelineName}).Inc()
				}
				//keep error in message index
				errs[message.index] = err
			}
		}(sinkCh)
	}
	for idx, message := range messages {
		// producer.Produce can be changed in to producer.produceBatch later once API is ready for production scale
		message := &sarama.ProducerMessage{
			Topic: tk.topic,
			Value: sarama.ByteEncoder(message.Payload),
		}
		sinkCh <- &sinkMessage{index: idx, message: message}
	}
	close(sinkCh)
	wg.Wait()
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
