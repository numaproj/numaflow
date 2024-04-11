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
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	sinkforward "github.com/numaproj/numaflow/pkg/sinks/forward"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// ToKafka produce the output to a kafka sinks.
type ToKafka struct {
	name         string
	pipelineName string
	producer     sarama.AsyncProducer
	connected    bool
	topic        string
	isdf         *sinkforward.DataForward
	kafkaSink    *dfv1.KafkaSink
	log          *zap.SugaredLogger
}

type Option func(*ToKafka) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(t *ToKafka) error {
		t.log = log
		return nil
	}
}

// NewToKafka returns ToKafka type.
func NewToKafka(vertexInstance *dfv1.VertexInstance,
	fromBuffer isb.BufferReader,
	fetchWatermark fetch.Fetcher,
	publishWatermark publish.Publisher,
	idleManager wmb.IdleManager,
	opts ...Option) (*ToKafka, error) {

	kafkaSink := vertexInstance.Vertex.Spec.Sink.Kafka
	toKafka := new(ToKafka)
	// apply options for kafka sink
	for _, o := range opts {
		if err := o(toKafka); err != nil {
			return nil, err
		}
	}

	// set default logger
	if toKafka.log == nil {
		toKafka.log = logging.NewLogger()
	}
	toKafka.log = toKafka.log.With("sinkType", "kafka").With("topic", kafkaSink.Topic)
	toKafka.name = vertexInstance.Vertex.Spec.Name
	toKafka.pipelineName = vertexInstance.Vertex.Spec.PipelineName
	toKafka.topic = kafkaSink.Topic
	toKafka.kafkaSink = kafkaSink

	forwardOpts := []sinkforward.Option{sinkforward.WithLogger(toKafka.log)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sinkforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
		if x.RetryInterval != nil {
			forwardOpts = append(forwardOpts, sinkforward.WithRetryInterval(x.RetryInterval.Duration))
		}
	}

	f, err := sinkforward.NewDataForward(vertexInstance, fromBuffer, toKafka, fetchWatermark, publishWatermark, idleManager, forwardOpts...)
	if err != nil {
		return nil, err
	}
	toKafka.isdf = f
	producer, err := connect(kafkaSink)
	if err != nil {
		return nil, err
	}
	toKafka.producer = producer
	toKafka.connected = true
	return toKafka, nil
}

func connect(kafkaSink *dfv1.KafkaSink) (sarama.AsyncProducer, error) {
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
	if s := kafkaSink.SASL; s != nil {
		if sasl, err := util.GetSASL(s); err != nil {
			return nil, err
		} else {
			config.Net.SASL = *sasl
		}
	}
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer(kafkaSink.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer. %w", err)
	}
	return producer, nil
}

// GetName returns the name.
func (tk *ToKafka) GetName() string {
	return tk.name
}

// GetPartitionIdx returns the partition index.
// for sink it is always 0.
func (tk *ToKafka) GetPartitionIdx() int32 {
	return 0
}

// Write writes to the kafka topic.
func (tk *ToKafka) Write(_ context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	errs := make([]error, len(messages))
	for i := 0; i < len(errs); i++ {
		errs[i] = fmt.Errorf("unknown error")
	}
	if !tk.connected {
		producer, err := connect(tk.kafkaSink)
		if err != nil {
			for i := 0; i < len(errs); i++ {
				errs[i] = fmt.Errorf("failed to get kafka producer, %w", err)
			}
			return nil, errs
		}
		tk.producer = producer
		tk.connected = true
	}
	done := make(chan struct{})
	timeout := time.After(5 * time.Second)
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
				// Need to close and recreate later because the successes and errors channels might be unclean
				_ = tk.producer.Close()
				tk.connected = false
				kafkaSinkWriteTimeouts.With(map[string]string{metrics.LabelVertex: tk.name, metrics.LabelPipeline: tk.pipelineName}).Inc()
				close(done)
				return
			default:
			}
		}
	}()
	for index, msg := range messages {
		message := &sarama.ProducerMessage{
			Topic:    tk.topic,
			Value:    sarama.ByteEncoder(msg.Payload),
			Metadata: index, // Use metadata to identify if it succeeds or fails in the async return.
		}
		tk.producer.Input() <- message
	}
	<-done
	for _, err := range errs {
		if err != nil {
			kafkaSinkWriteErrors.With(map[string]string{metrics.LabelVertex: tk.name, metrics.LabelPipeline: tk.pipelineName}).Inc()
		} else {
			kafkaSinkWriteCount.With(map[string]string{metrics.LabelVertex: tk.name, metrics.LabelPipeline: tk.pipelineName}).Inc()
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
